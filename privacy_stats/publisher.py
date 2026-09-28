"""统一隐私发布入口。

发布与扣费在单个 SQLite 事务内原子完成：

1. 以 ```request_id``` 幂等检查：同一请求（同 request_id、同版本、同机制、
   同参数）重复提交，直接返回首次发布结果，**不重新加噪、不再次扣费**。
   request_id 相同但参数不同则拒绝，防止幂等键被顶替成不同查询。
2. 校验预算是否足够；不足则整笔回滚并抛 ```BudgetExhaustedError```，
   绝不产生带噪结果。
3. 预算足够时，在同一事务内：计算真实值 → 加噪 → 写发布记录 → 扣预算。
   任何一步失败整体回滚，不会出现“结果发了但没扣费”或反之。

进程重启后状态仍在 SQLite 中：重放旧 request_id 命中既有发布，
因此不会重复发放；新版本上的新查询使用新 request_id，正常计费。
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any

from .budget import BudgetLedger
from .mechanisms import Mechanism, get_mechanism
from .noise import RandomSource
from .versions import DataVersionStore, DataVault


@dataclass(frozen=True)
class PublishedResult:
    request_id: str
    version_id: str
    mechanism: str
    params: dict[str, Any]
    noisy_result: int
    noisy_value_raw: float
    epsilon_spent: float
    budget_remaining: float
    reused: bool
    privacy_note: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "version_id": self.version_id,
            "mechanism": self.mechanism,
            "params": self.params,
            "noisy_result": self.noisy_result,
            "noisy_value_raw": self.noisy_value_raw,
            "epsilon_spent": self.epsilon_spent,
            "budget_remaining": self.budget_remaining,
            "reused": self.reused,
            "privacy_note": self.privacy_note,
        }


DuplicateRequestResult = PublishedResult

_PRIVACY_NOTE = (
    "本结果为差分隐私加噪计数（拉普拉斯机制），"
    "是随机化估计值，不是真实精确数量；请勿对外表述为精确计数。"
)


class Publisher:
    """唯一的统计发布入口。"""

    def __init__(
        self,
        conn: sqlite3.Connection,
        versions: DataVersionStore,
        ledger: BudgetLedger,
        rng: RandomSource,
    ):
        self._conn = conn
        self._versions = versions
        self._ledger = ledger
        self._rng = rng
        self._vault = DataVault(versions)
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS releases (
                request_id     TEXT PRIMARY KEY,
                version_id     TEXT NOT NULL,
                mechanism      TEXT NOT NULL,
                params_j       TEXT NOT NULL,
                noisy_result   INTEGER NOT NULL,
                noisy_raw      REAL NOT NULL,
                epsilon_spent  REAL NOT NULL,
                rng_state      TEXT NOT NULL,
                privacy_note   TEXT NOT NULL,
                created_at     TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """
        )
        self._conn.commit()

    def publish(
        self,
        request_id: str,
        version_id: str,
        mechanism_name: str,
        params: dict[str, Any] | None = None,
    ) -> PublishedResult:
        if not isinstance(request_id, str) or not request_id:
            raise ValueError("request_id 必须是非空字符串")
        params = dict(params or {})
        mechanism: Mechanism = get_mechanism(mechanism_name)
        self._versions.get(version_id)  # 版本不存在则抛 KeyError

        existing = self._fetch_release(request_id)
        if existing is not None:
            self._assert_same(existing, version_id, mechanism_name, params)
            return self._to_reused_result(existing)

        # 开事务前先做参数校验（如列是否存在），非法参数不消耗预算、不耗熵。
        self._validate_params(mechanism, version_id, params)

        self._conn.execute("BEGIN IMMEDIATE")
        try:
            # 事务内二次幂等检查，防止并发首次提交。
            existing = self._fetch_release(request_id)
            if existing is not None:
                self._conn.execute("COMMIT")
                self._assert_same(existing, version_id, mechanism_name, params)
                return self._to_reused_result(existing)

            true_value, noisy_value, _meta = mechanism.run(
                self._vault, version_id, params, self._rng
            )
            noisy_result = int(round(noisy_value))
            remaining_after = self._ledger.spend(
                request_id=request_id,
                version_id=version_id,
                mechanism=mechanism_name,
                epsilon=mechanism.epsilon,
            )
            self._conn.execute(
                """
                INSERT INTO releases
                    (request_id, version_id, mechanism, params_j,
                     noisy_result, noisy_raw, epsilon_spent, rng_state,
                     privacy_note)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    request_id,
                    version_id,
                    mechanism_name,
                    json.dumps(params, ensure_ascii=False, sort_keys=True),
                    noisy_result,
                    float(noisy_value),
                    float(mechanism.epsilon),
                    self._rng.seed_state(),
                    _PRIVACY_NOTE,
                ),
            )
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

        return PublishedResult(
            request_id=request_id,
            version_id=version_id,
            mechanism=mechanism_name,
            params=params,
            noisy_result=noisy_result,
            noisy_value_raw=float(noisy_value),
            epsilon_spent=float(mechanism.epsilon),
            budget_remaining=remaining_after,
            reused=False,
            privacy_note=_PRIVACY_NOTE,
        )

    def _to_reused_result(self, existing: dict) -> PublishedResult:
        return PublishedResult(
            request_id=existing["request_id"],
            version_id=existing["version_id"],
            mechanism=existing["mechanism"],
            params=json.loads(existing["params_j"]),
            noisy_result=existing["noisy_result"],
            noisy_value_raw=existing["noisy_raw"],
            epsilon_spent=0.0,
            budget_remaining=self._ledger.status().remaining,
            reused=True,
            privacy_note=existing["privacy_note"],
        )

    def _validate_params(self, mechanism: Mechanism, version_id: str, params: dict):
        # 用一次性随机源试跑，只验证参数合法性，不消耗正式随机源的熵。
        from .noise import DeterministicRandomSource

        mechanism.run(
            self._vault, version_id, params, DeterministicRandomSource(0)
        )

    def _fetch_release(self, request_id: str):
        cur = self._conn.execute(
            """
            SELECT request_id, version_id, mechanism, params_j,
                   noisy_result, noisy_raw, privacy_note
              FROM releases WHERE request_id = ?
            """,
            (request_id,),
        )
        columns = [d[0] for d in cur.description]
        row = cur.fetchone()
        return dict(zip(columns, row)) if row else None

    @staticmethod
    def _assert_same(
        existing: dict, version_id: str, mechanism_name: str, params: dict
    ):
        if (
            existing["version_id"] != version_id
            or existing["mechanism"] != mechanism_name
            or json.loads(existing["params_j"]) != params
        ):
            raise ValueError(
                f"request_id={existing['request_id']} 已绑定其他查询，"
                "同一 request_id 必须对应同一版本、机制与参数"
            )
