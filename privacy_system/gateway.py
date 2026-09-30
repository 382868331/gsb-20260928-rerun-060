"""隐私预算网关：唯一的统计发布入口。

设计要点
---------
1. 原始数据只在网关进程内通过数据版本号间接访问，调用方拿不到原始行；
2. 所有统计操作（第一版为有限种计数）都必须走 :meth:`PrivacyGateway.release`，
   发布与扣预算在同一个原子账本事务中完成，要么同时成功要么同时回滚；
3. 预算合成规则为顺序组合（sequential composition）：总 ε 预算固定，
   每次发布按该次 ε 累减，剩余不足则拒绝：拒绝决定也持久化（不扣预算），
   重试/重开后仍能与“尚未发布”区分；
4. 重复请求（相同 ``request_id``）幂等返回首次发布结果，不再扣预算；
   新查询（不同 ``request_id``）即使参数完全相同也视为独立新发布并扣预算；
5. 已提交事务在磁盘持久化（``fsync`` 由 SQLite 保证），重启后
   预算账与已发布结果仍在，因此不会重复发放。
6. 多个网关实例（含不同进程）共用同一账本文件时，事务由文件锁串行化，
   锁内重读磁盘最新状态，跨实例的重复请求对应同一发布事实，
   并发争用最后一点预算不会双双越额成功。
7. 每个请求有四种公开状态：尚未发布、已发布但调用者未收到
   （提交落盘后、交付前退出）、已交付、预算不足；分别由
   :meth:`request_status` 查询、:meth:`recover_release` 接续。
"""

from __future__ import annotations

import json
import random
import threading
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any

from . import catalog
from .errors import BudgetExhausted, DuplicateRequest, UnknownVersion
from .fictional_data import fictional_dataset
from .ledger import FileLedger
from .mechanism import laplace_noise

MECHANISM = "laplace"
COMPOSITION = "sequential_additive"
DELTA = 0.0

# 请求的公开生命周期状态（只描述发布事实，不含任何统计值）：
# 尚未发布 / 已发布落盘但调用者尚未确认收到 / 调用者已收到 / 预算不足被拒。
REQUEST_NOT_RELEASED = "not_released"
REQUEST_PENDING_DELIVERY = "published_pending_delivery"
REQUEST_DELIVERED = "delivered"
REQUEST_BUDGET_EXHAUSTED = "budget_exhausted"


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
    delivered: bool
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
        """提交一次隐私发布请求（查重、预算校验、落盘整体原子）。

        - 相同 ``request_id`` 且已发布：幂等返回首次结果（参数必须一致），
          不重复扣预算、不重新加噪，即“重复请求/接续”；
        - 相同 ``request_id`` 但此前因预算不足被拒：再次抛出同样的拒绝，
          拒绝决定已持久化，仍然不扣预算；
        - 不同 ``request_id``：即使查询内容相同也算“新查询”，独立扣预算。

        多个网关实例（含不同进程）共用同一账本时，事务通过账本文件锁
        串行化，因此同一请求只会产生同一份发布事实、只扣一次预算，
        争用最后一点预算的请求也不会双双越额成功。
        """
        with self._lock:
            return self._release_locked(
                request_id, version, operation, params, epsilon=epsilon
            )

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
        if version not in self._versions:
            if version not in self._ledger.state["versions"]:
                raise UnknownVersion(f"未知数据版本: {version!r}")
            raise UnknownVersion(
                f"版本 {version!r} 的原始数据不在本进程内存中，请重新注册（账本记录仍有效）"
            )

        version_data = self._versions.get(version)
        created_at = _utc_now()
        action: dict[str, Any] = {}

        def _apply(state: dict[str, Any]) -> None:
            # 版本登记本身也在同一本账里，锁内读取的是最新状态。
            if version not in state["versions"]:
                raise UnknownVersion(f"未知数据版本: {version!r}")
            if version_data is None:
                raise UnknownVersion(
                    f"版本 {version!r} 的原始数据不在本进程内存中，请重新注册（账本记录仍有效）"
                )

            prior_rejection = state["rejections"].get(request_id)
            if prior_rejection is not None:
                if (
                    prior_rejection["version"] != version
                    or prior_rejection["operation"] != operation
                    or prior_rejection["params"] != params
                ):
                    raise DuplicateRequest(
                        f"request_id={request_id!r} 已用于不同的发布参数，拒绝执行"
                    )
                action["kind"] = "rejected"
                action["detail"] = prior_rejection
                return
            prior = state["releases"].get(request_id)
            if prior is not None:
                if (
                    prior["version"] != version
                    or prior["operation"] != operation
                    or prior["params"] != params
                ):
                    raise DuplicateRequest(
                        f"request_id={request_id!r} 已用于不同的发布参数，拒绝执行"
                    )
                action["kind"] = "replay"
                action["record"] = prior
                action["remaining"] = (
                    state["total_epsilon"] - state["spent_epsilon"]
                )
                prior["delivered"] = True
                return

            total = state["total_epsilon"]
            spent = state["spent_epsilon"]
            remaining = total - spent
            if epsilon > remaining + 1e-12:
                # 持久化拒绝决定（不扣预算、不生成噪声）：
                # 重试/重开后仍能明确识别为“预算不足”。
                state["rejections"][request_id] = {
                    "version": version,
                    "operation": operation,
                    "params": params,
                    "epsilon": epsilon,
                    "remaining": remaining,
                    "total": total,
                    "created_at": created_at,
                }
                action["kind"] = "rejected"
                action["detail"] = state["rejections"][request_id]
                return

            true_count = op.compute(version_data.data, params)
            noise = laplace_noise(self._rng, op.sensitivity, epsilon)
            state["spent_epsilon"] = spent + epsilon
            state["releases"][request_id] = {
                "version": version,
                "operation": operation,
                "params": params,
                "true_count": true_count,
                "noise": noise,
                "noisy_value": float(true_count) + noise,
                "epsilon": epsilon,
                "mechanism": MECHANISM,
                "created_at": created_at,
                # 结果已随本次提交持久化；只有在调用者实际拿到回执后
                # 才由 mark_delivered() 标记为已交付。
                "delivered": False,
            }
            action["kind"] = "published"
            action["record"] = state["releases"][request_id]
            action["remaining"] = total - spent - epsilon

        self._ledger.transaction(_apply)

        if action["kind"] == "rejected":
            detail = action["detail"]
            raise BudgetExhausted(
                f"预算不足：本次需要 epsilon={detail['epsilon']:g}，"
                f"剩余 {detail['remaining']:g}（总额 {detail['total']:g}）"
            )

        record = action["record"]
        result = ReleaseResult(
            request_id=request_id,
            version=record["version"],
            operation=record["operation"],
            params=dict(record["params"]),
            value=record["noisy_value"],
            mechanism=record["mechanism"],
            epsilon_spent=0.0 if action["kind"] == "replay" else epsilon,
            epsilon_remaining=float(action["remaining"]),
            delta=DELTA,
            is_replay=action["kind"] == "replay",
            # 能构造出回执就意味着本次调用即将把结果交给调用者，
            # 紧接着的 _mark_delivered 使账本状态与之一致。
            delivered=True,
            created_at=record["created_at"],
        )
        self._mark_delivered(request_id)
        return result

    # ---------- 提交中途退出后的查询与接续 ----------

    def request_status(self, request_id: str) -> str:
        """查询请求当前所处的公开状态。

        - ``not_released``：账本中没有该请求（尚未发布，也未被拒）；
        - ``published_pending_delivery``：结果已原子落盘，但尚未被标记交付
          （典型场景：提交落盘后、返回调用者前进程退出）；
        - ``delivered``：结果已经实际交付给调用者；
        - ``budget_exhausted``：此前因预算不足被拒（拒绝已持久化，不扣预算）。

        该接口只返回状态字符串，不泄露真实计数或噪声。
        """
        if not request_id or not isinstance(request_id, str):
            raise ValueError("request_id 必须是非空字符串")
        state = self._ledger.state
        if request_id in state["rejections"]:
            return REQUEST_BUDGET_EXHAUSTED
        record = state["releases"].get(request_id)
        if record is None:
            return REQUEST_NOT_RELEASED
        return REQUEST_DELIVERED if record.get("delivered") else REQUEST_PENDING_DELIVERY

    def recover_release(self, request_id: str) -> ReleaseResult:
        """接续一个已提交的请求：取回其发布事实并标记为已交付。

        - 请求尚未发布：抛 :class:`KeyError`（调用者应重新 :meth:`release`）；
        - 已发布（无论此前是否已交付）：幂等返回同一份公开结果并标记交付，
          不重新加噪、不重复扣预算；
        - 此前因预算不足被拒：抛出持久化的 :class:`BudgetExhausted`。
        """
        if not request_id or not isinstance(request_id, str):
            raise ValueError("request_id 必须是非空字符串")
        with self._lock:
            outcome: dict[str, Any] = {}

            def _apply(state: dict[str, Any]) -> None:
                rejection = state["rejections"].get(request_id)
                if rejection is not None:
                    outcome["kind"] = "rejected"
                    outcome["detail"] = rejection
                    return
                record = state["releases"].get(request_id)
                if record is None:
                    outcome["kind"] = "missing"
                    return
                record["delivered"] = True
                outcome["kind"] = "replay"
                outcome["record"] = record
                outcome["remaining"] = (
                    state["total_epsilon"] - state["spent_epsilon"]
                )

            self._ledger.transaction(_apply)

            if outcome["kind"] == "missing":
                raise KeyError(f"不存在发布记录: {request_id!r}")
            if outcome["kind"] == "rejected":
                detail = outcome["detail"]
                raise BudgetExhausted(
                    f"预算不足：本次需要 epsilon={detail['epsilon']:g}，"
                    f"剩余 {detail['remaining']:g}（总额 {detail['total']:g}）"
                )
            record = outcome["record"]
            return ReleaseResult(
                request_id=request_id,
                version=record["version"],
                operation=record["operation"],
                params=dict(record["params"]),
                value=record["noisy_value"],
                mechanism=record["mechanism"],
                epsilon_spent=0.0,
                epsilon_remaining=float(outcome["remaining"]),
                delta=DELTA,
                is_replay=True,
                delivered=True,
                created_at=record["created_at"],
            )

    def mark_delivered(self, request_id: str) -> None:
        """确认调用者已经收到某个已发布请求的结果。

        幂等：重复确认无副作用；请求不存在时抛 :class:`KeyError`。
        提交（:meth:`release` / :meth:`recover_release`）成功返回前会自动确认；
        若提交落盘后、返回调用者前进程退出，记录会停留在
        ``published_pending_delivery``，由调用者用 :meth:`recover_release` 接续。
        """
        def _apply(state: dict[str, Any]) -> None:
            record = state["releases"].get(request_id)
            if record is None:
                raise KeyError(f"不存在发布记录: {request_id!r}")
            record["delivered"] = True

        self._ledger.transaction(_apply)

    def _mark_delivered(self, request_id: str) -> None:
        def _apply(state: dict[str, Any]) -> None:
            state["releases"][request_id]["delivered"] = True

        self._ledger.transaction(_apply)

    def mark_pending_delivery(self, request_id: str) -> None:
        """故障注入/测试辅助：把已发布记录回置为“已发布但调用者未收到”。

        用于模拟发布事务已提交、但结果尚未交付调用者时进程退出的窗口。
        """
        def _apply(state: dict[str, Any]) -> None:
            record = state["releases"].get(request_id)
            if record is None:
                raise KeyError(f"不存在发布记录: {request_id!r}")
            record["delivered"] = False

        self._ledger.transaction(_apply)

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
