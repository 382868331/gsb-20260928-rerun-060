"""隐私预算网关：唯一的统计发布入口。

设计要点
---------
1. 原始数据只在网关进程内通过数据版本号间接访问，调用方拿不到原始行；
2. 所有统计操作（第一版为有限种计数）都必须走 :meth:`PrivacyGateway.release`，
   发布与扣预算在同一次文件锁保护的原子提交中完成，要么同时成功要么同时回滚；
3. 预算合成规则为顺序组合（sequential composition）：总 ε 预算固定，
   每次发布按该次 ε 累减，剩余不足则拒绝，不产生任何结果也不扣预算；
4. 重复请求（相同 ``request_id``）幂等返回首次发布结果，不再扣预算；
   新查询（不同 ``request_id``）即使参数完全相同也视为独立新发布并扣预算；
5. 公开账本只保存带噪结果；真实计数与本次噪声保存在独立私有审计文件中；
6. 已提交记录在磁盘持久化，重启后预算账与已发布结果仍在，因此不会重复发放。
"""

from __future__ import annotations

import json
import math
import random
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any

from . import catalog
from .errors import BudgetExhausted, DuplicateRequest, UnknownVersion
from .fictional_data import fictional_dataset
from .ledger import AuditLog, FileLedger
from .mechanism import laplace_noise

MECHANISM = "laplace"
COMPOSITION = "sequential_additive"
DELTA = 0.0


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass(frozen=True)
class ReleaseResult:
    """一次发布的回执。``value`` 是带噪估计值，不是真实精确数量。"""

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
        audit_path = None if db_path == ":memory:" else db_path + ".private-audit.json"
        self._audit = AuditLog(audit_path)

    # ---------- 生命周期 ----------

    def close(self) -> None:
        self._ledger.close()
        self._audit.close()

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

        true_count = op.compute(self._versions[version].data, params)
        created_at = _utc_now()
        noise: float | None = None
        outcome: dict[str, Any] | None = None

        def _apply(state: dict[str, Any]) -> None:
            nonlocal noise, outcome
            prior = state["releases"].get(request_id)
            if prior is not None:
                self._validate_replay(request_id, version, operation, params, epsilon, prior)
                outcome = {"replay": True, "record": prior}
                return

            total = state["total_epsilon"]
            spent = state["spent_epsilon"]
            remaining = total - spent
            if epsilon > remaining + 1e-12:
                raise BudgetExhausted(
                    f"预算不足：本次需要 epsilon={epsilon:g}，剩余 {remaining:g}（总额 {total:g}）"
                )
            noise = laplace_noise(self._rng, op.sensitivity, epsilon)
            noisy_value = float(true_count) + noise
            public_record = {
                "version": version,
                "operation": operation,
                "params": params,
                "noisy_value": noisy_value,
                "epsilon": epsilon,
                "mechanism": MECHANISM,
                "created_at": created_at,
            }
            audit_record = {
                **public_record,
                "request_id": request_id,
                "true_count": true_count,
                "noise": noise,
            }
            self._audit.put_release(audit_record)
            state["spent_epsilon"] = spent + epsilon
            state["releases"][request_id] = public_record
            outcome = {"replay": False, "record": public_record}

        try:
            self._ledger.commit(_apply)
        except BaseException:
            if request_id not in self._ledger.state["releases"]:
                self._audit.remove_release(request_id)
            raise
        assert outcome is not None
        record = outcome["record"]
        is_replay = bool(outcome["replay"])

        return ReleaseResult(
            request_id=request_id,
            version=record["version"],
            operation=record["operation"],
            params=dict(record["params"]),
            value=record["noisy_value"],
            mechanism=record["mechanism"],
            epsilon_spent=0.0 if is_replay else epsilon,
            epsilon_remaining=float(self.budget()["remaining_epsilon"]),
            delta=DELTA,
            is_replay=is_replay,
            created_at=record["created_at"],
        )

    def _validate_replay(
        self,
        request_id: str,
        version: str,
        operation: str,
        params: dict[str, str],
        epsilon: float,
        prior: dict[str, Any],
    ) -> None:
        if (
            prior["version"] != version
            or prior["operation"] != operation
            or prior["params"] != params
            or not math.isclose(float(prior["epsilon"]), float(epsilon), rel_tol=0.0, abs_tol=1e-12)
        ):
            raise DuplicateRequest(
                "request_id 已用于不同的发布请求，拒绝执行"
            )

    def list_releases(self) -> list[dict[str, Any]]:
        """公开发布列表：只暴露带噪结果与非敏感发布元数据。"""
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

    def _audit_record(self, request_id: str) -> dict[str, Any]:
        """返回私有审计证据；仅应在具备审计文件权限的内部环境调用。"""
        record = self._audit.state["releases"].get(request_id)
        if record is None:
            raise KeyError(request_id)
        return dict(record)
