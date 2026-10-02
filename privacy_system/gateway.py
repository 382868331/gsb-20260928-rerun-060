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

MODE_ALL = "all"
MODE_PARTIAL = "partial"

ITEM_PUBLISHED = "published"
ITEM_REUSED = "reused"
ITEM_NOT_PUBLISHED = "not_published"
ITEM_CANCELLED = "cancelled"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _fact_key(version: str, operation: str, params: dict[str, str]) -> str:
    """统计发布事实的稳定身份：显示名称不参与，筛选范围/操作参与。"""
    payload = json.dumps(
        [version, operation, params], sort_keys=True, ensure_ascii=False, default=str
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]


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
                "operation": operation,
                "params": params,
                "fact_key": _fact_key(version, operation, params),
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

    # ---------- 多统计批次 ----------

    def release_batch(
        self,
        batch_id: str,
        version: str,
        items: list[dict[str, Any]],
        *,
        mode: str = MODE_ALL,
    ) -> dict[str, Any]:
        """一次申请发布多项统计。

        ``mode='all'`` 为整组同时发布：总预算不足时整组拒绝，不产生任何
        可观察单项结果。``mode='partial'`` 逐项原子提交；余额不足时保留
        已发布项，后续项标记为 ``not_published``。

        每个 item 需包含 ``item_id``、``operation``、``epsilon``；可包含
        ``params`` 与 ``source_request_id``。``source_request_id`` 表示显式
        复用一条版本、操作、参数完全相同的既有发布事实；只按显示名称相同
        不会复用。
        """
        with self._lock:
            normalized = self._normalize_batch_items(version, items)
            if mode not in (MODE_ALL, MODE_PARTIAL):
                raise ValueError("mode 必须是 'all' 或 'partial'")

            existing = self._ledger.snapshot()["batches"].get(batch_id)
            if existing is not None:
                self._assert_same_batch(existing, version, mode, normalized)
                return self._claim_batch(batch_id)

            if mode == MODE_ALL:
                return self._release_all_batch(batch_id, version, normalized)
            return self._release_partial_batch(batch_id, version, normalized)

    def _normalize_batch_items(
        self, version: str, items: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        if not isinstance(items, list | tuple) or not items:
            raise ValueError("items 必须是非空列表")
        normalized: list[dict[str, Any]] = []
        seen: set[str] = set()
        for raw in items:
            if not isinstance(raw, dict):
                raise ValueError("批次项目必须是字典")
            item_id = str(raw.get("item_id", ""))
            if not item_id or item_id in seen:
                raise ValueError("item_id 必须是非空且批次内唯一")
            seen.add(item_id)
            operation = raw.get("operation")
            params = dict(raw.get("params") or {})
            epsilon = float(raw["epsilon"])
            if epsilon <= 0:
                raise ValueError("epsilon 必须为正数")
            op = catalog.get_operation(operation)
            op.validate_params(params)
            source = raw.get("source_request_id")
            normalized.append(
                {
                    "item_id": item_id,
                    "version": version,
                    "operation": operation,
                    "params": params,
                    "epsilon": epsilon,
                    "source_request_id": source,
                    "fact_key": _fact_key(version, operation, params),
                }
            )
        return normalized

    @staticmethod
    def _assert_same_batch(
        existing: dict[str, Any],
        version: str,
        mode: str,
        items: list[dict[str, Any]],
    ) -> None:
        old_spec = [
            {
                "item_id": item["item_id"],
                "version": item["version"],
                "operation": item["operation"],
                "params": item["params"],
                "epsilon": item["epsilon"],
                "source_request_id": item.get("source_request_id"),
            }
            for item in existing["items"]
        ]
        new_spec = [
            {
                "item_id": item["item_id"],
                "version": item["version"],
                "operation": item["operation"],
                "params": item["params"],
                "epsilon": item["epsilon"],
                "source_request_id": item.get("source_request_id"),
            }
            for item in items
        ]
        if existing["version"] != version or existing["mode"] != mode or old_spec != new_spec:
            raise DuplicateRequest(f"batch_id={existing['batch_id']!r} 已用于不同批次")

    def _new_item_rng(self, seed: int, index: int) -> random.Random:
        return random.Random(f"batch-noise:{seed}:{index}")

    def _release_all_batch(
        self, batch_id: str, version: str, items: list[dict[str, Any]]
    ) -> dict[str, Any]:
        seed = self._rng.getrandbits(64)
        batch_record = {
            "batch_id": batch_id,
            "version": version,
            "mode": MODE_ALL,
            "items": [],
            "delivered": False,
            "created_at": _utc_now(),
            "noise_seed": seed,
        }
        outcome: dict[str, Any] = {}

        def _apply(state: dict[str, Any]) -> None:
            if version not in state["versions"] or version not in self._versions:
                raise UnknownVersion(f"未知数据版本: {version!r}")
            prepared = self._prepare_batch_items(state, version, items)
            required = sum(
                item["epsilon"] for item in prepared if item["status"] == ITEM_PUBLISHED
            )
            remaining = state["total_epsilon"] - state["spent_epsilon"]
            if required > remaining + 1e-12:
                raise BudgetExhausted(
                    f"整组预算不足：需要 epsilon={required:g}，剩余 {remaining:g}"
                )

            batch = dict(batch_record)
            batch["items"] = []
            state["spent_epsilon"] = state["spent_epsilon"] + required
            for index, item in enumerate(prepared):
                if item["status"] == ITEM_PUBLISHED:
                    op = catalog.get_operation(item["operation"])
                    epsilon = item["epsilon"]
                    noise = laplace_noise(
                        self._new_item_rng(seed, index), op.sensitivity, epsilon
                    )
                    true_count = op.compute(self._versions[version].data, item["params"])
                    release_id = self._batch_release_id(batch_id, item["item_id"])
                    record = {
                        "version": version,
                        "operation": item["operation"],
                        "params": item["params"],
                        "fact_key": item["fact_key"],
                        "true_count": true_count,
                        "noise": noise,
                        "noisy_value": float(true_count) + noise,
                        "epsilon": epsilon,
                        "mechanism": MECHANISM,
                        "created_at": _utc_now(),
                        "delivered": False,
                        "batch_id": batch_id,
                        "item_id": item["item_id"],
                    }
                    state["releases"][release_id] = record
                    item["release_request_id"] = release_id
                    item["noisy_value"] = record["noisy_value"]
                batch["items"].append(item)
            state["batches"][batch_id] = batch
            outcome["batch"] = batch
            if batch_id in self._drop_after_commit:
                outcome["do_drop"] = True

        self._ledger.commit(_apply)
        if outcome.get("do_drop"):
            self._drop_after_commit.discard(batch_id)
            raise ReleaseInterrupted(
                f"batch_id={batch_id!r} 已整组发布并扣减预算，但在返回前连接中断；"
                "请用同一 batch_id 调用 claim_batch() 接续"
            )
        return self._claim_batch(batch_id)

    def _prepare_batch_items(
        self, state: dict[str, Any], version: str, items: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        prepared = [dict(item) for item in items]
        for item in prepared:
            source_id = item.get("source_request_id")
            if source_id is None:
                item["status"] = ITEM_PUBLISHED
                item["release_request_id"] = None
                continue
            source = state["releases"].get(source_id)
            if source is None:
                raise KeyError(f"复用的发布不存在: {source_id!r}")
            source_key = source.get("fact_key") or _fact_key(
                source["version"], source["operation"], source["params"]
            )
            if source_key != item["fact_key"] or source["version"] != version:
                raise DuplicateRequest(
                    f"{source_id!r} 与 {item['item_id']!r} 是不同发布事实，禁止复用"
                )
            item["status"] = ITEM_REUSED
            item["release_request_id"] = source_id
            item["noisy_value"] = source["noisy_value"]
        return prepared

    @staticmethod
    def _batch_release_id(batch_id: str, item_id: str) -> str:
        return f"batch:{batch_id}:{item_id}"

    def _release_partial_batch(
        self, batch_id: str, version: str, items: list[dict[str, Any]]
    ) -> dict[str, Any]:
        seed = self._rng.getrandbits(64)

        def _init(state: dict[str, Any]) -> None:
            if version not in state["versions"] or version not in self._versions:
                raise UnknownVersion(f"未知数据版本: {version!r}")
            state["batches"][batch_id] = {
                "batch_id": batch_id,
                "version": version,
                "mode": MODE_PARTIAL,
                "delivered": False,
                "created_at": _utc_now(),
                "noise_seed": seed,
                "items": [
                    {
                        **item,
                        "status": ITEM_NOT_PUBLISHED,
                        "release_request_id": None,
                        "noisy_value": None,
                    }
                    for item in items
                ],
            }

        self._ledger.commit(_init)
        return self.continue_batch(batch_id)

    def continue_batch(self, batch_id: str) -> dict[str, Any]:
        """接续部分批次：已发布项保持结果与预算，尝试尚未发布且未取消项。"""
        with self._lock:
            state = self._ledger.snapshot()
            batch = state["batches"].get(batch_id)
            if batch is None:
                raise KeyError(f"不存在批次: {batch_id!r}")
            if batch["mode"] != MODE_PARTIAL:
                return self._claim_batch(batch_id)

            for index, item in enumerate(batch["items"]):
                if item["status"] != ITEM_NOT_PUBLISHED:
                    continue
                try:
                    self._release_partial_item(batch_id, index, item)
                except BudgetExhausted:
                    continue
            return self._claim_batch(batch_id)

    def _release_partial_item(
        self, batch_id: str, index: int, item: dict[str, Any]
    ) -> None:
        outcome: dict[str, Any] = {}

        def _apply(state: dict[str, Any]) -> None:
            batch = state["batches"][batch_id]
            current = batch["items"][index]
            if current["status"] not in (ITEM_NOT_PUBLISHED,):
                outcome["already"] = True
                return

            source_id = current.get("source_request_id")
            if source_id is not None:
                source = state["releases"].get(source_id)
                if source is None:
                    raise KeyError(f"复用的发布不存在: {source_id!r}")
                source_key = source.get("fact_key") or _fact_key(
                    source["version"], source["operation"], source["params"]
                )
                if source_key != current["fact_key"] or source["version"] != batch["version"]:
                    raise DuplicateRequest("不同发布事实不能复用")
                current["status"] = ITEM_REUSED
                current["release_request_id"] = source_id
                current["noisy_value"] = source["noisy_value"]
                outcome["item"] = dict(current)
                return

            epsilon = current["epsilon"]
            remaining = state["total_epsilon"] - state["spent_epsilon"]
            if epsilon > remaining + 1e-12:
                raise BudgetExhausted(
                    f"项目 {current['item_id']!r} 预算不足：需要 {epsilon:g}，剩余 {remaining:g}"
                )
            op = catalog.get_operation(current["operation"])
            rng = self._new_item_rng(batch["noise_seed"], index)
            noise = laplace_noise(rng, op.sensitivity, epsilon)
            true_count = op.compute(
                self._versions[batch["version"]].data, current["params"]
            )
            release_id = self._batch_release_id(batch_id, current["item_id"])
            state["releases"][release_id] = {
                "version": current["version"],
                "operation": current["operation"],
                "params": current["params"],
                "fact_key": current["fact_key"],
                "true_count": true_count,
                "noise": noise,
                "noisy_value": float(true_count) + noise,
                "epsilon": epsilon,
                "mechanism": MECHANISM,
                "created_at": _utc_now(),
                "delivered": False,
                "batch_id": batch_id,
                "item_id": current["item_id"],
            }
            state["spent_epsilon"] = state["spent_epsilon"] + epsilon
            current["status"] = ITEM_PUBLISHED
            current["release_request_id"] = release_id
            current["noisy_value"] = state["releases"][release_id]["noisy_value"]
            outcome["item"] = dict(current)

        self._ledger.commit(_apply)

    def batch_status(self, batch_id: str) -> dict[str, Any]:
        """公开查询批次状态，不返回统计值或噪声。"""
        batch = self._ledger.snapshot()["batches"].get(batch_id)
        if batch is None:
            return {"batch_id": batch_id, "status": "not_released", "items": []}
        return {
            "batch_id": batch_id,
            "mode": batch["mode"],
            "delivered": bool(batch.get("delivered")),
            "items": [
                {
                    "item_id": item["item_id"],
                    "status": item["status"],
                    "release_request_id": item.get("release_request_id"),
                    "epsilon_requested": item.get("epsilon", 0.0),
                    "epsilon_spent": item.get("epsilon", 0.0)
                    if item["status"] == ITEM_PUBLISHED
                    else 0.0,
                }
                for item in batch["items"]
            ],
        }

    def claim_batch(self, batch_id: str) -> dict[str, Any]:
        """领取批次结果；同一批次重复领取返回同一结果且不重复扣预算。"""
        with self._lock:
            return self._claim_batch(batch_id)

    def _claim_batch(self, batch_id: str) -> dict[str, Any]:
        before = self._ledger.snapshot()["batches"].get(batch_id)
        first_delivery = before is not None and not before.get("delivered")

        def _mark(state: dict[str, Any]) -> None:
            batch = state["batches"].get(batch_id)
            if batch is None:
                raise KeyError(f"不存在批次: {batch_id!r}")
            batch["delivered"] = True
            for item in batch["items"]:
                release_id = item.get("release_request_id")
                if release_id and release_id in state["releases"]:
                    state["releases"][release_id]["delivered"] = True

        self._ledger.commit(_mark)
        return self._batch_receipt(batch_id, is_replay=not first_delivery)

    def _batch_receipt(self, batch_id: str, *, is_replay: bool = False) -> dict[str, Any]:
        state = self._ledger.snapshot()
        batch = state["batches"][batch_id]
        budget = self.budget()
        return {
            "batch_id": batch_id,
            "mode": batch["mode"],
            "is_replay": is_replay,
            "items": [
                {
                    "item_id": item["item_id"],
                    "status": item["status"],
                    "version": item["version"],
                    "operation": item["operation"],
                    "params": dict(item["params"]),
                    "release_request_id": item.get("release_request_id"),
                    "noisy_value": item.get("noisy_value"),
                    "epsilon_requested": item.get("epsilon", 0.0),
                    "epsilon_spent": item.get("epsilon", 0.0)
                    if item["status"] == ITEM_PUBLISHED
                    else 0.0,
                }
                for item in batch["items"]
            ],
            "epsilon_remaining": float(budget["remaining_epsilon"]),
            "created_at": batch["created_at"],
        }

    def cancel_batch_items(self, batch_id: str, item_ids: list[str] | set[str] | tuple[str, ...]) -> dict[str, Any]:
        """取消部分批次中尚未发布的项目；已发布/复用项目不可取消或退款。"""
        wanted = set(item_ids)

        def _apply(state: dict[str, Any]) -> None:
            batch = state["batches"].get(batch_id)
            if batch is None:
                raise KeyError(f"不存在批次: {batch_id!r}")
            if batch["mode"] != MODE_PARTIAL:
                raise ValueError("只有部分发布批次允许取消单项")
            by_id = {item["item_id"]: item for item in batch["items"]}
            missing = wanted - set(by_id)
            if missing:
                raise KeyError(f"批次项目不存在: {sorted(missing)}")
            for item_id in wanted:
                if by_id[item_id]["status"] == ITEM_NOT_PUBLISHED:
                    by_id[item_id]["status"] = ITEM_CANCELLED

        self._ledger.commit(_apply)
        return self.batch_status(batch_id)

    def reconcile_batch(self, batch_id: str) -> dict[str, Any]:
        """公开对账：只包含发布身份与预算去向，不包含统计值或噪声。"""
        state = self._ledger.snapshot()
        batch = state["batches"].get(batch_id)
        if batch is None:
            return {"batch_id": batch_id, "status": "not_released", "items": []}
        spent = 0.0
        reused = 0
        items = []
        for item in batch["items"]:
            item_spent = (
                0.0 if item["status"] == ITEM_REUSED else float(item.get("epsilon", 0.0))
            )
            if item["status"] == ITEM_PUBLISHED:
                spent += item_spent
            if item["status"] == ITEM_REUSED:
                reused += 1
            items.append(
                {
                    "item_id": item["item_id"],
                    "status": item["status"],
                    "release_request_id": item.get("release_request_id"),
                    "epsilon_spent": item_spent if item["status"] == ITEM_PUBLISHED else 0.0,
                    "budget_destination": item["status"],
                }
            )
        return {
            "batch_id": batch_id,
            "mode": batch["mode"],
            "items": items,
            "new_epsilon_spent": spent,
            "reused_release_count": reused,
            "cancelled_count": sum(1 for item in items if item["status"] == ITEM_CANCELLED),
            "unpublished_count": sum(1 for item in items if item["status"] == ITEM_NOT_PUBLISHED),
        }

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
