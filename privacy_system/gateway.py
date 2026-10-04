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

import hashlib
import json
import random
import threading
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any

from . import catalog
from .errors import (
    BudgetExhausted,
    DuplicateRequest,
    InvalidPlanState,
    ReleaseInterrupted,
    UnknownPlan,
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

# 发布安排的兑现模式。
PLAN_MODE_ATOMIC = "atomic"   # 整组一同兑现：预算不足则一项都不发布
PLAN_MODE_STAGED = "staged"   # 分阶段逐项兑现：已发布部分永久保留，未发布部分可取消

# 安排与其统计项的可观察状态。
PLAN_STATUS_APPROVED = "approved"                  # 已批准，尚未兑现任何一项
PLAN_STATUS_PARTIAL = "partially_fulfilled"        # 分阶段兑现中：部分已发布
PLAN_STATUS_FULFILLED = "fulfilled"                # 全部统计项已发布
PLAN_STATUS_CANCELLED = "cancelled"                # 未发布部分已取消（已发布部分保留）
ITEM_PENDING = "pending"
ITEM_FULFILLED = "fulfilled"
ITEM_CANCELLED = "cancelled"


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

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


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
            }
            state["spent_epsilon"] = spent + epsilon
            state["releases"][request_id] = record
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

    # ---------- 发布安排：预演 → 批准 → 兑现 → 领取 ----------
    #
    # 一个“发布安排”把一组相关统计绑定为一个整体：
    #   1. preview_plan  预演：只给出统计身份、可共享的既有发布事实与预算去向，
    #      不产生任何可领取结果，也不透露精确统计或随机噪声；
    #   2. approve_plan  批准：把安排身份与批准时的数据版本（含指纹）一起入账，
    #      之后到达的新数据属于新版本，必须另行研究产生独立安排；
    #   3. fulfill_plan  兑现：atomic 整组一同发布（预算不足则一项都不发布），
    #      staged 分阶段逐项发布（已发布部分永久保留，未发布部分可取消）；
    #   4. claim_plan    领取：只返回已兑现项的公开结果。
    #
    # 安排身份 plan_id 由「数据版本 + 版本指纹 + 兑现模式 + 全部统计项
    # （操作/参数/ε）」的哈希决定，与显示名称无关：改变筛选范围或统计含义
    # 必然得到新身份，同名安排不会被复用。

    @staticmethod
    def _normalize_plan_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not items:
            raise ValueError("发布安排至少需要一项统计")
        normalized = []
        for item in items:
            operation = item.get("operation")
            op = catalog.get_operation(operation)
            params = dict(item.get("params") or {})
            op.validate_params(params)
            epsilon = float(item.get("epsilon", 1.0))
            if epsilon <= 0:
                raise ValueError("每项统计的 epsilon 必须为正数")
            normalized.append(
                {"operation": operation, "params": params, "epsilon": epsilon}
            )
        return normalized

    @staticmethod
    def _validate_plan_mode(mode: str) -> None:
        if mode not in (PLAN_MODE_ATOMIC, PLAN_MODE_STAGED):
            raise ValueError(f"未知兑现模式: {mode!r}（应为 atomic 或 staged）")

    @staticmethod
    def _plan_identity(
        version: str, fingerprint: str, mode: str, items: list[dict[str, Any]]
    ) -> str:
        payload = json.dumps(
            {
                "version": version,
                "fingerprint": fingerprint,
                "mode": mode,
                "items": items,
            },
            sort_keys=True,
            ensure_ascii=False,
        )
        return "plan-" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]

    @staticmethod
    def _find_shared_release(
        state: dict[str, Any], version: str, item: dict[str, Any]
    ) -> str | None:
        """在既有发布事实中查找与统计项完全同身份（版本/操作/参数/ε）的记录。

        只匹配**已经发布**的事实（releases 账本），因此一项尚未发布的统计
        即使被多个安排共同引用，也不会在此泄露另一安排的结果。
        """
        for rid, record in state["releases"].items():
            if (
                record["version"] == version
                and record["operation"] == item["operation"]
                and record["params"] == item["params"]
                and abs(record["epsilon"] - item["epsilon"]) <= 1e-12
            ):
                return rid
        return None

    def preview_plan(
        self,
        version: str,
        items: list[dict[str, Any]],
        *,
        mode: str = PLAN_MODE_ATOMIC,
        name: str = "",
    ) -> dict[str, Any]:
        """预演一组统计的发布安排：只显示身份、可共享事实与预算去向。

        返回内容刻意**不含**任何带噪值、真实计数或噪声，也不产生可领取结果；
        对每一项给出：统计身份、若兑现将新消耗的 ε（fresh_epsilon）、
        以及可共享的既有发布事实（shared_from，仅引用其身份）。
        """
        with self._lock:
            normalized = self._normalize_plan_items(items)
            self._validate_plan_mode(mode)
            state = self._ledger.snapshot()
            if version not in state["versions"]:
                raise UnknownVersion(f"未知数据版本: {version!r}")
            fingerprint = state["versions"][version]["fingerprint"]
            plan_id = self._plan_identity(version, fingerprint, mode, normalized)
            remaining = state["total_epsilon"] - state["spent_epsilon"]
            preview_items = []
            total_fresh = 0.0
            for index, item in enumerate(normalized):
                shared_from = self._find_shared_release(state, version, item)
                fresh = 0.0 if shared_from is not None else item["epsilon"]
                total_fresh += fresh
                preview_items.append(
                    {
                        "index": index,
                        "operation": item["operation"],
                        "params": dict(item["params"]),
                        "epsilon": item["epsilon"],
                        "shared_from": shared_from,
                        "fresh_epsilon": fresh,
                    }
                )
            existing = state["plans"].get(plan_id)
            return {
                "plan_id": plan_id,
                "name": name,
                "version": version,
                "fingerprint": fingerprint,
                "mode": mode,
                "items": preview_items,
                "total_fresh_epsilon": total_fresh,
                "remaining_epsilon": remaining,
                "budget_sufficient": total_fresh <= remaining + 1e-12,
                "existing_plan_status": existing["status"] if existing else None,
            }

    def approve_plan(
        self,
        version: str,
        items: list[dict[str, Any]],
        *,
        mode: str = PLAN_MODE_ATOMIC,
        name: str = "",
    ) -> str:
        """批准一个发布安排并入账，返回其身份 plan_id。

        安排身份由内容（版本指纹 + 模式 + 统计项）决定：相同内容重复批准
        幂等返回同一身份；显示名称 name 不参与身份，仅为展示用途。
        批准把安排绑定到**批准时**的数据版本与指纹，之后的新数据不影响它。
        """
        with self._lock:
            normalized = self._normalize_plan_items(items)
            self._validate_plan_mode(mode)
            outcome: dict[str, Any] = {}

            def _apply(state: dict[str, Any]) -> None:
                if version not in state["versions"]:
                    raise UnknownVersion(f"未知数据版本: {version!r}")
                fingerprint = state["versions"][version]["fingerprint"]
                plan_id = self._plan_identity(version, fingerprint, mode, normalized)
                existing = state["plans"].get(plan_id)
                if existing is not None:
                    if existing["status"] == PLAN_STATUS_CANCELLED:
                        raise InvalidPlanState(
                            f"安排 {plan_id!r} 已取消；相同内容的安排身份不变，不能重新批准"
                        )
                    outcome["plan_id"] = plan_id
                    return
                state["plans"][plan_id] = {
                    "plan_id": plan_id,
                    "name": name,
                    "version": version,
                    "fingerprint": fingerprint,
                    "mode": mode,
                    "items": [
                        {
                            "operation": item["operation"],
                            "params": dict(item["params"]),
                            "epsilon": item["epsilon"],
                            "status": ITEM_PENDING,
                            "request_id": None,
                            "shared_from": None,
                            "epsilon_charged": 0.0,
                        }
                        for item in normalized
                    ],
                    "status": PLAN_STATUS_APPROVED,
                    "created_at": _utc_now(),
                    "events": [
                        {"event": "approved", "at": _utc_now(), "epsilon_charged": 0.0}
                    ],
                }
                outcome["plan_id"] = plan_id

            self._ledger.commit(_apply)
            return outcome["plan_id"]

    def fulfill_plan(
        self, plan_id: str, item_indexes: list[int] | None = None
    ) -> dict[str, Any]:
        """兑现获批安排（或其指定项），返回兑现后的公开安排状态。

        - atomic：整组一同兑现。组内可共享既有发布事实的项直接引用、
          不再扣预算；其余项合计 ε 超出余额时抛 BudgetExhausted，
          **整组一项都不发布、不扣预算**（原子回滚）。
        - staged：必须给出 item_indexes，本次只兑现这些项（同一笔原子提交）。
          已发布项永久保留其事实与预算消耗；未发布项可取消。

        实际消耗预算的兑现会在统计项上记录 request_id；共享既有事实的项
        记录 shared_from 且 epsilon_charged=0，绝不重复消耗。
        重复兑现同一项幂等返回既有事实，不重新加噪、不二次扣账。
        """
        with self._lock:

            def _apply(state: dict[str, Any]) -> None:
                plan = state["plans"].get(plan_id)
                if plan is None:
                    raise UnknownPlan(f"不存在的发布安排: {plan_id!r}")
                if plan["status"] == PLAN_STATUS_CANCELLED:
                    raise InvalidPlanState(f"安排 {plan_id!r} 已取消，不能兑现")
                if plan["version"] not in self._versions:
                    raise UnknownVersion(
                        f"版本 {plan['version']!r} 的原始数据不在本进程内存中，"
                        "请重新注册（账本记录仍有效）"
                    )
                if plan["mode"] == PLAN_MODE_ATOMIC:
                    if item_indexes is not None:
                        raise ValueError("atomic 安排只能整组兑现，不能指定子集")
                    targets = list(range(len(plan["items"])))
                else:
                    if not item_indexes:
                        raise ValueError("staged 安排必须指定本次兑现的统计项下标")
                    targets = sorted(set(item_indexes))
                    for index in targets:
                        if not 0 <= index < len(plan["items"]):
                            raise ValueError(f"统计项下标越界: {index}")
                        if plan["items"][index]["status"] == ITEM_CANCELLED:
                            raise InvalidPlanState(f"统计项 {index} 已取消，不能兑现")

                todo = [
                    i for i in targets if plan["items"][i]["status"] == ITEM_PENDING
                ]
                remaining = state["total_epsilon"] - state["spent_epsilon"]
                shared: dict[int, str] = {}
                fresh_total = 0.0
                for index in todo:
                    item = plan["items"][index]
                    rid = self._find_shared_release(state, plan["version"], item)
                    if rid is not None:
                        shared[index] = rid
                    else:
                        fresh_total += item["epsilon"]
                if fresh_total > remaining + 1e-12:
                    raise BudgetExhausted(
                        f"预算不足：本次兑现需新消耗 epsilon={fresh_total:g}，"
                        f"剩余 {remaining:g}；未产生任何发布，也未扣预算"
                    )

                spent = state["spent_epsilon"]
                for index in todo:
                    item = plan["items"][index]
                    if index in shared:
                        item["status"] = ITEM_FULFILLED
                        item["request_id"] = shared[index]
                        item["shared_from"] = shared[index]
                        item["epsilon_charged"] = 0.0
                        plan["events"].append(
                            {
                                "event": "fulfilled_item",
                                "at": _utc_now(),
                                "item_index": index,
                                "request_id": shared[index],
                                "shared_from": shared[index],
                                "epsilon_charged": 0.0,
                            }
                        )
                        continue
                    request_id = f"{plan_id}:item:{index}"
                    op = catalog.get_operation(item["operation"])
                    true_count = op.compute(
                        self._versions[plan["version"]].data, item["params"]
                    )
                    noise = laplace_noise(self._rng, op.sensitivity, item["epsilon"])
                    state["releases"][request_id] = {
                        "version": plan["version"],
                        "operation": item["operation"],
                        "params": dict(item["params"]),
                        "true_count": true_count,
                        "noise": noise,
                        "noisy_value": float(true_count) + noise,
                        "epsilon": item["epsilon"],
                        "mechanism": MECHANISM,
                        "created_at": _utc_now(),
                        "delivered": True,
                        "plan_id": plan_id,
                        "item_index": index,
                    }
                    spent += item["epsilon"]
                    item["status"] = ITEM_FULFILLED
                    item["request_id"] = request_id
                    item["shared_from"] = None
                    item["epsilon_charged"] = item["epsilon"]
                    plan["events"].append(
                        {
                            "event": "fulfilled_item",
                            "at": _utc_now(),
                            "item_index": index,
                            "request_id": request_id,
                            "shared_from": None,
                            "epsilon_charged": item["epsilon"],
                        }
                    )
                state["spent_epsilon"] = spent
                if all(i["status"] == ITEM_FULFILLED for i in plan["items"]):
                    plan["status"] = PLAN_STATUS_FULFILLED
                elif any(i["status"] == ITEM_FULFILLED for i in plan["items"]):
                    plan["status"] = PLAN_STATUS_PARTIAL

            self._ledger.commit(_apply)
            return self.plan_status(plan_id)

    def cancel_plan(self, plan_id: str) -> dict[str, Any]:
        """取消安排中所有尚未兑现的统计项；已发布部分永久保留。

        已发布项的发布事实与预算消耗不变；未发布项标记为已取消，
        之后不能再兑现。重复取消幂等。
        """
        with self._lock:

            def _apply(state: dict[str, Any]) -> None:
                plan = state["plans"].get(plan_id)
                if plan is None:
                    raise UnknownPlan(f"不存在的发布安排: {plan_id!r}")
                if plan["status"] == PLAN_STATUS_CANCELLED:
                    return
                for item in plan["items"]:
                    if item["status"] == ITEM_PENDING:
                        item["status"] = ITEM_CANCELLED
                plan["status"] = PLAN_STATUS_CANCELLED
                plan["events"].append(
                    {"event": "cancelled", "at": _utc_now(), "epsilon_charged": 0.0}
                )

            self._ledger.commit(_apply)
            return self.plan_status(plan_id)

    def plan_status(self, plan_id: str) -> dict[str, Any]:
        """公开查看安排状态：身份、各项状态与预算去向。

        已兑现项附带公开带噪值（本就是公开事实）；未兑现项只有状态，
        没有任何数值。不含真实计数与噪声。
        """
        state = self._ledger.snapshot()
        plan = state["plans"].get(plan_id)
        if plan is None:
            raise UnknownPlan(f"不存在的发布安排: {plan_id!r}")
        items = []
        for index, item in enumerate(plan["items"]):
            entry = {
                "index": index,
                "operation": item["operation"],
                "params": dict(item["params"]),
                "epsilon": item["epsilon"],
                "status": item["status"],
                "request_id": item["request_id"],
                "shared_from": item["shared_from"],
                "epsilon_charged": item["epsilon_charged"],
            }
            if item["status"] == ITEM_FULFILLED:
                record = state["releases"][item["request_id"]]
                entry["noisy_value"] = record["noisy_value"]
                entry["mechanism"] = record["mechanism"]
                entry["created_at"] = record["created_at"]
            items.append(entry)
        return {
            "plan_id": plan["plan_id"],
            "name": plan["name"],
            "version": plan["version"],
            "fingerprint": plan["fingerprint"],
            "mode": plan["mode"],
            "status": plan["status"],
            "created_at": plan["created_at"],
            "items": items,
        }

    def claim_plan(self, plan_id: str) -> list[ReleaseResult]:
        """领取已兑现统计项的公开结果；未兑现项不产生任何结果。

        只返回已发布事实（带噪值，不含真实计数与噪声）；共享项返回其
        引用的同一份公开发布，不重复扣预算、不重新加噪。
        """
        state = self._ledger.snapshot()
        plan = state["plans"].get(plan_id)
        if plan is None:
            raise UnknownPlan(f"不存在的发布安排: {plan_id!r}")
        budget = self.budget()
        results = []
        for index, item in enumerate(plan["items"]):
            if item["status"] != ITEM_FULFILLED:
                continue
            record = state["releases"][item["request_id"]]
            results.append(
                ReleaseResult(
                    request_id=item["request_id"],
                    version=record["version"],
                    operation=record["operation"],
                    params=dict(record["params"]),
                    value=record["noisy_value"],
                    mechanism=record["mechanism"],
                    epsilon_spent=item["epsilon_charged"],
                    epsilon_remaining=float(budget["remaining_epsilon"]),
                    delta=DELTA,
                    is_replay=True,
                    created_at=record["created_at"],
                )
            )
        return results

    def list_plans(self) -> list[dict[str, Any]]:
        """公开列出全部发布安排的身份与状态（不含任何统计数值）。"""
        state = self._ledger.snapshot()
        return [
            {
                "plan_id": plan["plan_id"],
                "name": plan["name"],
                "version": plan["version"],
                "fingerprint": plan["fingerprint"],
                "mode": plan["mode"],
                "status": plan["status"],
                "created_at": plan["created_at"],
                "item_count": len(plan["items"]),
                "fulfilled_count": sum(
                    1 for i in plan["items"] if i["status"] == ITEM_FULFILLED
                ),
                "epsilon_charged_total": sum(
                    i["epsilon_charged"] for i in plan["items"]
                ),
            }
            for _, plan in sorted(
                state["plans"].items(), key=lambda kv: kv[1]["created_at"]
            )
        ]

    def audit_plan_events(self) -> list[dict[str, Any]]:
        """公开审计：全部安排的批准/兑现/取消事件及预算变化。

        只含发布事实（哪次兑现消耗了预算、哪项共享了既有事实）与 ε 变化，
        不含真实计数、噪声或任何未发布统计的数值。
        """
        state = self._ledger.snapshot()
        events = []
        for plan_id, plan in sorted(state["plans"].items()):
            for event in plan["events"]:
                events.append({"plan_id": plan_id, **event})
        events.sort(key=lambda e: e["at"])
        return events

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
        )

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
