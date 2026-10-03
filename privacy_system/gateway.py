"""隐私预算网关：唯一的统计发布入口。

设计要点
---------
1. 原始数据只在网关进程内通过数据版本号间接访问，调用方拿不到原始行；
2. 所有统计操作（第一版为有限种计数）都必须走 :meth:`PrivacyGateway.release`，
   发布与扣预算在同一次原子账本提交中完成，要么同时成功要么同时回滚；
3. 预算合成规则为顺序组合（sequential composition）：总 ε 预算固定，
   每次发布按该次 ε 累减，剩余不足则拒绝，不产生任何结果也不扣预算；
4. 重复请求（相同 ``request_id``）幂等返回首次发布结果，不再扣预算；
   新查询（不同 ``request_id``）即使参数完全相同也视为独立新发布并扣预算；
5. 多个网关实例/进程共用同一份账本时，查重、预算校验与落盘在同一把
   跨进程文件锁内、基于磁盘最新状态完成，因此不会各自重复发放或互相覆盖；
6. 已提交结果在磁盘持久化（临时文件 + ``fsync`` + 原子 rename），重启后
   预算账与已发布结果仍在；即使结果在“提交成功后、返回调用者前”丢失，
   也可凭同一 ``request_id`` 查询/重放取回，绝不会再扣一次预算。
"""

from __future__ import annotations

import json
import hashlib
import random
import threading
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any

from . import catalog
from .errors import (
    BudgetExhausted,
    DuplicateRequest,
    ReleaseInterrupted,
    UnknownVersion,
)
from .fictional_data import fictional_dataset
from .ledger import FileLedger
from .mechanism import laplace_noise

MECHANISM = "laplace"
COMPOSITION = "sequential_additive"
DELTA = 0.0

# 请求生命周期的三种可观察状态（见 :meth:`PrivacyGateway.request_status`）。
STATUS_UNKNOWN = "not_released"        # 账本中没有该请求：尚未发布
STATUS_PUBLISHED = "published"        # 已发布且调用者已至少收到一次回执
STATUS_DELIVERED_UNKNOWN = "published_unacknowledged"  # 已发布，但尚无证据已送达

MODE_ALL = "all_at_once"
MODE_PHASED = "phased"

PLAN_APPROVED = "approved"
PLAN_FULFILLED = "fulfilled"
PLAN_PARTIAL = "partial"
PLAN_CANCELED = "canceled"

ITEM_APPROVED = "approved"
ITEM_PUBLISHED = "published"
ITEM_CLAIMED = "claimed"
ITEM_CANCELED = "canceled"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass(frozen=True)
class ReleaseResult:
    """一次发布的公开回执。``value`` 是带噪估计值，不是真实精确数量。

    公开回执**不包含本次噪声**：若同时给出带噪值与噪声，
    两者相减即可还原精确计数，因此噪声只留在内部账本中供审计。
    """

    request_id: str
    version: str
    operation: str
    params: dict[str, str]
    value: float
    mechanism: str
    epsilon_spent: float
    epsilon_remaining: float
    delta: float
    is_replay: bool
    created_at: str
    arrangement_id: str | None = None
    item_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _statistical_identity(
    version: str, fingerprint: str, operation: str, params: dict[str, str]
) -> str:
    """由数据版本、操作和完整筛选条件确定统计身份；显示名不参与身份。"""
    payload = json.dumps(
        {
            "version": version,
            "fingerprint": fingerprint,
            "operation": operation,
            "params": params,
        },
        sort_keys=True,
        ensure_ascii=False,
    )
    return "stat-" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


@dataclass(frozen=True)
class _Version:
    version: str
    description: str
    created_at: str
    fingerprint: str
    data: tuple[tuple[str, ...], tuple[dict, ...]]


class PrivacyGateway:
    """所有隐私统计发布的唯一入口。"""

    def __init__(
        self,
        db_path: str = ":memory:",
        *,
        total_epsilon: float = 10.0,
        rng: random.Random | None = None,
    ) -> None:
        if total_epsilon <= 0:
            raise ValueError("total_epsilon 必须为正数")
        self._rng = rng if rng is not None else random.Random()
        self._versions: dict[str, _Version] = {}
        self._ledger = FileLedger(db_path, total_epsilon, COMPOSITION)
        # 串行化发布：并发下的查重、加噪与扣账必须作为一个整体执行，
        # 否则两个相同 request_id 的并发请求可能都通过查重而重复扣账。
        self._lock = threading.RLock()
        # 故障注入：集合内 request_id 在“原子提交成功后、结果返回调用者前”
        # 模拟连接中断（抛出 ReleaseInterrupted），用于测试提交中途退出。
        # 此时预算已扣、结果已落盘，但 delivered 仍为 False。
        self._drop_after_commit: set[str] = set()

    # ---------- 生命周期 ----------

    def close(self) -> None:
        self._ledger.close()

    def __enter__(self) -> "PrivacyGateway":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ---------- 初始化与数据版本 ----------

    def register_version(
        self,
        version: str,
        data: tuple[tuple[str, ...], tuple[dict, ...]],
        *,
        description: str = "",
    ) -> str:
        """登记一个数据版本。原始数据不写入磁盘账本，只在内存中保留。"""
        import hashlib

        payload = json.dumps(
            [list(data[0]), [dict(r) for r in data[1]]],
            sort_keys=True,
            ensure_ascii=False,
            default=str,
        )
        fingerprint = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
        def _persist(state: dict[str, Any]) -> None:
            existing = state["versions"].get(version)
            if existing is not None:
                if existing["fingerprint"] != fingerprint:
                    raise ValueError(f"版本号 {version!r} 已绑定不同数据")
                return
            state["versions"][version] = {
                "description": description,
                "fingerprint": fingerprint,
                "created_at": _utc_now(),
            }

        self._ledger.commit(_persist)
        created_at = self._ledger.state["versions"][version]["created_at"]
        self._versions[version] = _Version(
            version=version,
            description=description,
            created_at=created_at,
            fingerprint=fingerprint,
            data=(tuple(data[0]), tuple(dict(r) for r in data[1])),
        )
        return fingerprint

    def register_fictional_v1(self) -> str:
        """登记第一版内置虚构数据，返回版本号。"""
        version = "fictional-v1"
        self.register_version(
            version,
            fictional_dataset(),
            description="内置虚构示例数据（10 行），非真实个人信息",
        )
        return version

    def list_versions(self) -> list[dict[str, str]]:
        return [
            {
                "version": name,
                "description": meta["description"],
                "fingerprint": meta["fingerprint"],
                "created_at": meta["created_at"],
            }
            for name, meta in sorted(self._ledger.state["versions"].items())
        ]

    # ---------- 预算查询 ----------

    def budget(self) -> dict[str, float | str]:
        state = self._ledger.state
        total = state["total_epsilon"]
        spent = state["spent_epsilon"]
        composition = state["composition"]
        return {
            "total_epsilon": total,
            "spent_epsilon": spent,
            "remaining_epsilon": total - spent,
            "composition": composition,
            "delta_total": DELTA,
        }

    # ---------- 发布（唯一统计入口） ----------

    def release(
        self,
        request_id: str,
        version: str,
        operation: str,
        params: dict[str, str] | None = None,
        *,
        epsilon: float = 1.0,
    ) -> ReleaseResult:
        """提交一次隐私发布请求。

        - 相同 ``request_id``：幂等返回首次结果（参数必须一致），不重复扣预算、
          不重新加噪，即“重复请求”；
        - 不同 ``request_id``：即使查询内容相同也算“新查询”，独立扣预算。
        """
        with self._lock:
            return self._release_locked(request_id, version, operation, params, epsilon=epsilon)

    def _release_locked(
        self,
        request_id: str,
        version: str,
        operation: str,
        params: dict[str, str] | None = None,
        *,
        epsilon: float = 1.0,
    ) -> ReleaseResult:
        if not request_id or not isinstance(request_id, str):
            raise ValueError("request_id 必须是非空字符串")
        if epsilon <= 0:
            raise ValueError("epsilon 必须为正数")
        params = dict(params or {})
        op = catalog.get_operation(operation)
        op.validate_params(params)
        # 所有事实性判断（版本、查重、参数冲突、预算）都放进账本提交闭包，
        # 在跨进程锁内基于磁盘最新状态执行：否则另一个实例可能已用同一
        # request_id 发布，或已耗尽余额，本实例的旧快照无法察觉。
        outcome: dict[str, Any] = {}

        def _apply(state: dict[str, Any]) -> None:
            prior = state["releases"].get(request_id)
            if prior is not None:
                # 查重命中（含本实例/其他实例/重启前的发布事实）：同一发布事实。
                if (
                    prior["version"] != version
                    or prior["operation"] != operation
                    or prior["params"] != params
                ):
                    raise DuplicateRequest(
                        f"request_id={request_id!r} 已用于不同的发布参数，拒绝执行"
                    )
                # 重放本身就是把结果送达调用者，标记已送达；不扣预算、不重新加噪。
                prior["delivered"] = True
                outcome["kind"] = "replay"
                outcome["record"] = prior
                return

            if version not in state["versions"]:
                raise UnknownVersion(f"未知数据版本: {version!r}")
            if version not in self._versions:
                raise UnknownVersion(
                    f"版本 {version!r} 的原始数据不在本进程内存中，请重新注册（账本记录仍有效）"
                )

            total = state["total_epsilon"]
            spent = state["spent_epsilon"]
            remaining = total - spent
            if epsilon > remaining + 1e-12:
                # 余额不足：闭包抛错，候选状态整体丢弃——不产生记录、不扣预算。
                raise BudgetExhausted(
                    f"预算不足：本次需要 epsilon={epsilon:g}，剩余 {remaining:g}（总额 {total:g}）"
                )

            true_count = op.compute(self._versions[version].data, params)
            noise = laplace_noise(self._rng, op.sensitivity, epsilon)
            record = {
                "version": version,
                "identity": _statistical_identity(
                    version,
                    self._versions[version].fingerprint,
                    operation,
                    params,
                ),
                "operation": operation,
                "params": params,
                "true_count": true_count,
                "noise": noise,
                "noisy_value": float(true_count) + noise,
                "epsilon": epsilon,
                "mechanism": MECHANISM,
                "created_at": _utc_now(),
                # delivered=False 表示“已发布、已扣预算，但尚无证据调用者已收到”。
                # 与 noisy_value/true_count/noise 在同一次原子提交里写入。
                "delivered": False,
                "arrangement_id": None,
                "item_id": None,
                "source_request_id": request_id,
                "reusable": True,
            }
            state["spent_epsilon"] = spent + epsilon
            state["releases"][request_id] = record
            state["budget_events"].append(
                {
                    "event_id": f"event-{len(state['budget_events']) + 1}",
                    "at": record["created_at"],
                    "kind": "release",
                    "arrangement_id": None,
                    "item_id": None,
                    "request_id": request_id,
                    "stat_identity": record["identity"],
                    "epsilon": epsilon,
                    "source_request_id": request_id,
                    "reason": "direct_release",
                }
            )
            outcome["kind"] = "new"
            outcome["record"] = record
            # 仅当本次确实完成了一笔新发布时，才触发一次性的“送达前中断”。
            if request_id in self._drop_after_commit:
                outcome["do_drop"] = True

        self._ledger.commit(_apply)

        if outcome.get("do_drop"):
            self._drop_after_commit.discard(request_id)
            # 提交已完成且持久化，但模拟结果在送达前丢失：不允许调用者
            # 误以为尚未发布。账本中 delivered 保持 False，可查询/重放取回。
            raise ReleaseInterrupted(
                f"request_id={request_id!r} 的结果已发布并扣减预算，但在返回前连接中断；"
                "请用同一 request_id 重放或调用 request_status() 取回结果"
            )

        record = outcome["record"]
        is_replay = outcome["kind"] == "replay"
        # 新发布走到这里即视为已送达；通过单独的标记提交把 delivered 落盘。
        # 重放的标记已随上面的提交完成。
        if not is_replay:
            self._mark_delivered(request_id)

        budget = self.budget()
        return ReleaseResult(
            request_id=request_id,
            version=record["version"],
            operation=record["operation"],
            params=dict(record["params"]),
            value=record["noisy_value"],
            mechanism=record["mechanism"],
            epsilon_spent=0.0 if is_replay else record["epsilon"],
            epsilon_remaining=float(budget["remaining_epsilon"]),
            delta=DELTA,
            is_replay=is_replay,
            created_at=record["created_at"],
            arrangement_id=record.get("arrangement_id"),
            item_id=record.get("item_id"),
        )

    def _mark_delivered(self, request_id: str) -> None:
        """把一次已发布请求标记为“调用者已收到”。仅更新送达标记，不动预算。"""

        def _apply(state: dict[str, Any]) -> None:
            record = state["releases"].get(request_id)
            if record is not None:
                record["delivered"] = True

        self._ledger.commit(_apply)

    # ---------- 提交中途退出：查询与接续 ----------

    def request_status(self, request_id: str) -> dict[str, Any]:
        """查询请求当前处于三种状态中的哪一种（不扣预算、不重新加噪）。

        返回字段 ``status``：

        * ``not_released``：账本中不存在该请求，尚未发布；
        * ``published_unacknowledged``：已发布且已扣预算，但没有证据表明
          调用者曾收到回执（提交成功后、返回前退出/中断）；
        * ``published``：已发布，且调用者至少收到过一次回执。

        已发布时附带公开发布信息（带噪值，不含真实计数与噪声）。
        预算不足是 :meth:`release` 的拒绝结果（不产生记录），不属于本状态机；
        对一个从未发布的请求随后因预算不足被拒绝，其状态仍是
        ``not_released``，而拒绝动作抛出 :class:`BudgetExhausted`。
        """
        record = self._ledger.snapshot()["releases"].get(request_id)
        if record is None:
            return {"request_id": request_id, "status": STATUS_UNKNOWN}
        status = STATUS_PUBLISHED if record.get("delivered") else STATUS_DELIVERED_UNKNOWN
        return {
            "request_id": request_id,
            "status": status,
            "version": record["version"],
            "operation": record["operation"],
            "params": dict(record["params"]),
            "noisy_value": record["noisy_value"],
            "epsilon": record["epsilon"],
            "mechanism": record["mechanism"],
            "created_at": record["created_at"],
        }

    def get_release(self, request_id: str) -> ReleaseResult | None:
        """按 ``request_id`` 取回既有发布的公开回执；不存在返回 ``None``。

        用于提交中途退出后的接续：已发布但未收到结果时，凭同一
        ``request_id`` 取回同一份公开结果（``is_replay=True``），不扣预算。
        取回即视为送达，``delivered`` 标记落盘。
        """
        record = self._ledger.snapshot()["releases"].get(request_id)
        if record is None:
            return None
        if not record.get("delivered"):
            self._mark_delivered(request_id)
        budget = self.budget()
        return ReleaseResult(
            request_id=request_id,
            version=record["version"],
            operation=record["operation"],
            params=dict(record["params"]),
            value=record["noisy_value"],
            mechanism=record["mechanism"],
            epsilon_spent=0.0,
            epsilon_remaining=float(budget["remaining_epsilon"]),
            delta=DELTA,
            is_replay=True,
            created_at=record["created_at"],
            arrangement_id=record.get("arrangement_id"),
            item_id=record.get("item_id"),
        )

    # ---------- 发布安排：预演、批准、兑现与领取 ----------

    def preview_arrangement(
        self,
        items: list[dict[str, Any]],
        *,
        mode: str = MODE_ALL,
    ) -> dict[str, Any]:
        """预演一组统计的预算去向；不落盘、不批准、不抽噪声、不产生结果。"""
        normalized = self._normalize_items(items)
        self._validate_mode(mode)
        state = self._ledger.snapshot()
        preview_items = []
        total_new_epsilon = 0.0
        total_reused_epsilon = 0.0

        for item in normalized:
            identity = _statistical_identity(
                item["version"],
                self._versions[item["version"]].fingerprint,
                item["operation"],
                item["params"],
            )
            source = self._find_release(state, identity)
            if source is None:
                budget_action = "new_release"
                source_request_id = None
                charged_here = item["epsilon"]
                charged_before = 0.0
                total_new_epsilon += item["epsilon"]
            else:
                budget_action = "reuse_existing_release"
                source_request_id = source[0]
                charged_here = 0.0
                charged_before = source[1]["epsilon"]
                total_reused_epsilon += source[1]["epsilon"]
            preview_items.append(
                {
                    "item_id": item["item_id"],
                    "label": item["label"],
                    "stat_identity": identity,
                    "version": item["version"],
                    "data_fingerprint": self._versions[item["version"]].fingerprint,
                    "operation": item["operation"],
                    "params": item["params"],
                    "existing_release_request_id": source_request_id,
                    "budget_action": budget_action,
                    "epsilon_requested": item["epsilon"],
                    "epsilon_charged_in_this_arrangement": charged_here,
                    "epsilon_already_charged_by_existing_release": charged_before,
                }
            )

        return {
            "mode": mode,
            "items": preview_items,
            "new_epsilon_required": total_new_epsilon,
            "existing_release_epsilon_reused": total_reused_epsilon,
            "remaining_epsilon_now": state["total_epsilon"] - state["spent_epsilon"],
            "would_fit_atomic_all_at_once": total_new_epsilon
            <= state["total_epsilon"] - state["spent_epsilon"] + 1e-12,
            "approved": False,
        }

    def approve_arrangement(
        self,
        arrangement_id: str,
        items: list[dict[str, Any]],
        *,
        mode: str = MODE_ALL,
    ) -> dict[str, Any]:
        """批准并冻结安排；此时仍不抽噪声、不发布结果，也不预留预算。"""
        if not arrangement_id or not isinstance(arrangement_id, str):
            raise ValueError("arrangement_id 必须是非空字符串")
        normalized = self._normalize_items(items)
        self._validate_mode(mode)
        identities = {
            _statistical_identity(
                item["version"],
                self._versions[item["version"]].fingerprint,
                item["operation"],
                item["params"],
            )
            for item in normalized
        }
        if len(identities) != len(normalized):
            raise ValueError("同一安排内不能重复登记同一统计身份")

        def _apply(state: dict[str, Any]) -> dict[str, Any]:
            existing = state["arrangements"].get(arrangement_id)
            if existing is not None:
                if existing["mode"] != mode or not self._same_item_specs(
                    existing["items"], normalized
                ):
                    raise DuplicateRequest(
                        f"arrangement_id={arrangement_id!r} 已用于不同安排，拒绝复用"
                    )
                return existing
            arrangement = {
                "arrangement_id": arrangement_id,
                "mode": mode,
                "status": PLAN_APPROVED,
                "approved_at": _utc_now(),
                "items": self._stored_items(normalized),
            }
            state["arrangements"][arrangement_id] = arrangement
            return arrangement

        return self._public_arrangement(self._ledger.commit(_apply))

    def fulfill_arrangement(self, arrangement_id: str) -> dict[str, Any]:
        """整组兑现：整组新增预算不足时原子失败，不产生任何一项新发布。"""
        with self._lock:
            def _apply(state: dict[str, Any]) -> dict[str, Any]:
                arrangement = self._require_arrangement(state, arrangement_id)
                if arrangement["mode"] != MODE_ALL:
                    raise ValueError("分阶段安排必须调用 fulfill_arrangement_item()")
                if arrangement["status"] != PLAN_APPROVED:
                    raise ValueError(f"安排当前状态为 {arrangement['status']!r}，不能整组兑现")
                self._ensure_item_identities(state, arrangement)
                needed = sum(
                    item["epsilon"]
                    for item in arrangement["items"]
                    if self._find_release(state, item["identity"]) is None
                )
                remaining = state["total_epsilon"] - state["spent_epsilon"]
                if needed > remaining + 1e-12:
                    raise BudgetExhausted(
                        f"整组预算不足：新增需要 epsilon={needed:g}，剩余 {remaining:g}"
                    )
                for item in arrangement["items"]:
                    self._publish_item(state, arrangement, item)
                arrangement["status"] = PLAN_FULFILLED
                return arrangement

            return self._public_arrangement(self._ledger.commit(_apply))

    def fulfill_arrangement_item(self, arrangement_id: str, item_id: str) -> dict[str, Any]:
        """兑现分阶段安排中的一项；已发布部分不会因后续失败或取消而回滚。"""
        with self._lock:
            def _apply(state: dict[str, Any]) -> dict[str, Any]:
                arrangement = self._require_arrangement(state, arrangement_id)
                if arrangement["mode"] != MODE_PHASED:
                    raise ValueError("整组安排不能逐项兑现")
                if arrangement["status"] == PLAN_CANCELED:
                    raise ValueError("安排已取消，不能继续兑现")
                self._ensure_item_identities(state, arrangement)
                item = self._require_item(arrangement, item_id)
                if item["status"] != ITEM_APPROVED:
                    return arrangement
                source = self._find_release(state, item["identity"])
                needed = 0.0 if source is not None else item["epsilon"]
                remaining = state["total_epsilon"] - state["spent_epsilon"]
                if needed > remaining + 1e-12:
                    raise BudgetExhausted(
                        f"该统计新增预算不足：需要 epsilon={needed:g}，剩余 {remaining:g}"
                    )
                self._publish_item(state, arrangement, item)
                arrangement["status"] = (
                    PLAN_FULFILLED
                    if all(published["status"] != ITEM_APPROVED for published in arrangement["items"])
                    else PLAN_PARTIAL
                )
                return arrangement

            return self._public_arrangement(self._ledger.commit(_apply))

    def claim_arrangement_item(
        self, arrangement_id: str, item_id: str
    ) -> ReleaseResult:
        """领取一项已经兑现的结果；不重新加噪、不重复扣预算。"""
        with self._lock:
            def _apply(state: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
                arrangement = self._require_arrangement(state, arrangement_id)
                item = self._require_item(arrangement, item_id)
                if item["status"] in (ITEM_APPROVED, ITEM_CANCELED):
                    raise ValueError("该统计尚未兑现，不能领取")
                record = state["releases"][item["source_request_id"]]
                item["status"] = ITEM_CLAIMED
                record["delivered"] = True
                return arrangement, record

            arrangement, record = self._ledger.commit(_apply)
            return self._release_result_for(record)

    def claim_arrangement(self, arrangement_id: str) -> list[ReleaseResult]:
        """领取安排中所有已经兑现且尚未通过该安排领取的结果。"""
        claimed = []
        arrangement = self.get_arrangement(arrangement_id)
        for item in arrangement["items"]:
            if item["status"] in (ITEM_PUBLISHED, ITEM_CLAIMED) and item["status"] != ITEM_CLAIMED:
                claimed.append(self.claim_arrangement_item(arrangement_id, item["item_id"]))
        return claimed

    def cancel_arrangement(self, arrangement_id: str) -> dict[str, Any]:
        """取消安排；整组模式只能在兑现前取消，分阶段模式只取消未兑现项。"""
        def _apply(state: dict[str, Any]) -> dict[str, Any]:
            arrangement = self._require_arrangement(state, arrangement_id)
            if arrangement["mode"] == MODE_ALL:
                if arrangement["status"] != PLAN_APPROVED:
                    raise ValueError("整组安排已兑现，发布事实和预算消耗永久保留")
                arrangement["status"] = PLAN_CANCELED
                for item in arrangement["items"]:
                    item["status"] = ITEM_CANCELED
            else:
                for item in arrangement["items"]:
                    if item["status"] == ITEM_APPROVED:
                        item["status"] = ITEM_CANCELED
                if all(item["status"] == ITEM_CANCELED for item in arrangement["items"]):
                    arrangement["status"] = PLAN_CANCELED
                elif any(item["status"] in (ITEM_PUBLISHED, ITEM_CLAIMED) for item in arrangement["items"]):
                    arrangement["status"] = PLAN_PARTIAL
            return arrangement

        return self._public_arrangement(self._ledger.commit(_apply))

    def get_arrangement(self, arrangement_id: str) -> dict[str, Any]:
        return self._public_arrangement(
            self._require_arrangement(self._ledger.snapshot(), arrangement_id)
        )

    def list_arrangements(self) -> list[dict[str, Any]]:
        return [
            self._public_arrangement(arrangement)
            for arrangement in self._ledger.snapshot()["arrangements"].values()
        ]

    def public_audit_log(self) -> list[dict[str, Any]]:
        """公开审计：只给发布事实对应的预算变化，不含精确统计或随机噪声。"""
        return [dict(event) for event in self._ledger.snapshot()["budget_events"]]

    def _normalize_items(self, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not isinstance(items, list) or not items:
            raise ValueError("items 必须是非空列表")
        normalized = []
        seen_item_ids: set[str] = set()
        for index, raw in enumerate(items):
            if not isinstance(raw, dict):
                raise ValueError("每个统计项必须是对象")
            item_id = str(raw.get("item_id") or f"item-{index + 1}")
            if item_id in seen_item_ids:
                raise ValueError(f"统计项 ID 重复: {item_id!r}")
            seen_item_ids.add(item_id)
            version = raw.get("version")
            operation = raw.get("operation")
            params = dict(raw.get("params") or {})
            epsilon = float(raw.get("epsilon", 1.0))
            if not version or not isinstance(version, str):
                raise ValueError("统计项的 version 必须是非空字符串")
            if version not in self._versions:
                raise UnknownVersion(
                    f"版本 {version!r} 的原始数据不在本进程内存中，请重新注册（账本记录仍有效）"
                )
            if epsilon <= 0:
                raise ValueError("每个统计项的 epsilon 必须为正数")
            op = catalog.get_operation(operation)
            op.validate_params(params)
            fingerprint = self._versions[version].fingerprint
            normalized.append(
                {
                    "item_id": item_id,
                    "label": str(raw.get("label") or item_id),
                    "version": version,
                    "fingerprint": fingerprint,
                    "operation": operation,
                    "params": params,
                    "epsilon": epsilon,
                    "identity": _statistical_identity(
                        version, fingerprint, operation, params
                    ),
                }
            )
        return normalized

    @staticmethod
    def _stored_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "item_id": item["item_id"],
                "label": item["label"],
                "version": item["version"],
                "fingerprint": item["fingerprint"],
                "operation": item["operation"],
                "params": dict(item["params"]),
                "epsilon": item["epsilon"],
                "identity": item["identity"],
                "status": ITEM_APPROVED,
                "source_request_id": None,
            }
            for item in items
        ]

    @staticmethod
    def _validate_mode(mode: str) -> None:
        if mode not in (MODE_ALL, MODE_PHASED):
            raise ValueError("mode 必须是 all_at_once 或 phased")

    @staticmethod
    def _same_item_specs(
        existing_items: list[dict[str, Any]], requested_items: list[dict[str, Any]]
    ) -> bool:
        fields = (
            "item_id",
            "label",
            "version",
            "fingerprint",
            "operation",
            "params",
            "epsilon",
            "identity",
        )
        if len(existing_items) != len(requested_items):
            return False
        return all(
            all(existing.get(field) == requested.get(field) for field in fields)
            for existing, requested in zip(existing_items, requested_items)
        )

    @staticmethod
    def _require_arrangement(state: dict[str, Any], arrangement_id: str) -> dict[str, Any]:
        arrangement = state["arrangements"].get(arrangement_id)
        if arrangement is None:
            raise KeyError(f"不存在发布安排: {arrangement_id!r}")
        return arrangement

    @staticmethod
    def _require_item(arrangement: dict[str, Any], item_id: str) -> dict[str, Any]:
        for item in arrangement["items"]:
            if item["item_id"] == item_id:
                return item
        raise KeyError(f"安排中不存在统计项: {item_id!r}")

    def _ensure_item_identities(
        self, state: dict[str, Any], arrangement: dict[str, Any]
    ) -> None:
        for item in arrangement["items"]:
            if item.get("version") not in self._versions:
                raise UnknownVersion(
                    f"版本 {item.get('version')!r} 的原始数据不在本进程内存中，请重新注册"
                )
            expected = _statistical_identity(
                item["version"],
                self._versions[item["version"]].fingerprint,
                item["operation"],
                item["params"],
            )
            if item.setdefault("identity", expected) != expected:
                raise ValueError("发布安排绑定的数据范围与当前版本不一致")

    @staticmethod
    def _find_release(
        state: dict[str, Any], identity: str
    ) -> tuple[str, dict[str, Any]] | None:
        def record_identity(record: dict[str, Any]) -> str | None:
            if record.get("identity"):
                return record["identity"]
            version_meta = state["versions"].get(record.get("version"))
            if version_meta is None:
                return None
            return _statistical_identity(
                record["version"],
                version_meta["fingerprint"],
                record["operation"],
                record["params"],
            )

        matches = [
            (request_id, record)
            for request_id, record in state["releases"].items()
            if record_identity(record) == identity
        ]
        if not matches:
            return None
        return sorted(matches, key=lambda pair: (pair[1]["created_at"], pair[0]))[0]

    def _publish_item(
        self,
        state: dict[str, Any],
        arrangement: dict[str, Any],
        item: dict[str, Any],
    ) -> None:
        source = self._find_release(state, item["identity"])
        if source is not None:
            source_request_id, source_record = source
            item["source_request_id"] = source_request_id
            item["status"] = ITEM_PUBLISHED
            state["budget_events"].append(
                {
                    "event_id": f"event-{len(state['budget_events']) + 1}",
                    "at": _utc_now(),
                    "kind": "reuse_existing_release",
                    "arrangement_id": arrangement["arrangement_id"],
                    "item_id": item["item_id"],
                    "request_id": None,
                    "stat_identity": item["identity"],
                    "epsilon": 0.0,
                    "source_request_id": source_request_id,
                    "reason": "shared_statistical_identity",
                }
            )
            return

        request_id = f"{arrangement['arrangement_id']}:{item['item_id']}"
        if request_id in state["releases"]:
            raise DuplicateRequest(f"发布请求 ID 已存在: {request_id!r}")
        version = item["version"]
        operation = item["operation"]
        params = dict(item["params"])
        epsilon = item["epsilon"]
        op = catalog.get_operation(operation)
        true_count = op.compute(self._versions[version].data, params)
        sensitivity = op.sensitivity
        noise = laplace_noise(self._rng, sensitivity, epsilon)
        created_at = _utc_now()
        record = {
            "version": version,
            "identity": item["identity"],
            "operation": operation,
            "params": params,
            "true_count": true_count,
            "noise": noise,
            "noisy_value": float(true_count) + noise,
            "epsilon": epsilon,
            "mechanism": MECHANISM,
            "created_at": created_at,
            "delivered": False,
            "arrangement_id": arrangement["arrangement_id"],
            "item_id": item["item_id"],
            "source_request_id": request_id,
            "reusable": True,
        }
        state["spent_epsilon"] += epsilon
        state["releases"][request_id] = record
        item["source_request_id"] = request_id
        item["status"] = ITEM_PUBLISHED
        state["budget_events"].append(
            {
                "event_id": f"event-{len(state['budget_events']) + 1}",
                "at": created_at,
                "kind": "arrangement_release",
                "arrangement_id": arrangement["arrangement_id"],
                "item_id": item["item_id"],
                "request_id": request_id,
                "stat_identity": item["identity"],
                "epsilon": epsilon,
                "source_request_id": request_id,
                "reason": "new_statistical_release",
            }
        )

    def _release_result_for(self, record: dict[str, Any]) -> ReleaseResult:
        budget = self.budget()
        return ReleaseResult(
            request_id=record["source_request_id"],
            version=record["version"],
            operation=record["operation"],
            params=dict(record["params"]),
            value=record["noisy_value"],
            mechanism=record["mechanism"],
            epsilon_spent=0.0,
            epsilon_remaining=float(budget["remaining_epsilon"]),
            delta=DELTA,
            is_replay=True,
            created_at=record["created_at"],
            arrangement_id=record.get("arrangement_id"),
            item_id=record.get("item_id"),
        )

    @staticmethod
    def _public_arrangement(arrangement: dict[str, Any]) -> dict[str, Any]:
        return {
            "arrangement_id": arrangement["arrangement_id"],
            "mode": arrangement["mode"],
            "status": arrangement["status"],
            "approved_at": arrangement["approved_at"],
            "items": [
                {
                    "item_id": item["item_id"],
                    "label": item["label"],
                    "stat_identity": item.get("identity"),
                    "version": item["version"],
                    "data_fingerprint": item.get("fingerprint"),
                    "operation": item["operation"],
                    "params": dict(item["params"]),
                    "epsilon": item["epsilon"],
                    "status": item["status"],
                    "source_request_id": item.get("source_request_id"),
                }
                for item in arrangement["items"]
            ],
        }

    # ---------- 故障注入（测试用） ----------

    def fail_next_delivery(self, request_id: str) -> None:
        """测试辅助：让指定请求在提交落盘后、结果返回前“连接中断”。"""
        self._drop_after_commit.add(request_id)

    def list_releases(self) -> list[dict[str, Any]]:
        """公开发布列表：只暴露带噪值，不暴露噪声与真实计数。

        若同时给出带噪值与噪声，两者相减即还原精确计数，
        因此噪声字段只保留在内部账本中（见 :meth:`audit_release`）。
        """
        return [
            {
                "request_id": rid,
                "version": record["version"],
                "operation": record["operation"],
                "params": dict(record["params"]),
                "noisy_value": record["noisy_value"],
                "epsilon": record["epsilon"],
                "mechanism": record["mechanism"],
                "created_at": record["created_at"],
            }
            for rid, record in sorted(
                self._ledger.state["releases"].items(), key=lambda kv: kv[1]["created_at"]
            )
        ]

    def audit_release(self, request_id: str) -> dict[str, Any]:
        """**仅供内部审计**查看单次发布的完整依据（含真实计数与噪声）。

        该信息足以还原精确计数，禁止向查询者暴露或写入任何对外输出。
        """
        record = self._ledger.state["releases"].get(request_id)
        if record is None:
            raise KeyError(f"不存在发布记录: {request_id!r}")
        return {
            "request_id": request_id,
            "version": record["version"],
            "operation": record["operation"],
            "params": dict(record["params"]),
            "true_count": record["true_count"],
            "noise": record["noise"],
            "noisy_value": record["noisy_value"],
            "epsilon": record["epsilon"],
            "mechanism": record["mechanism"],
            "created_at": record["created_at"],
        }
