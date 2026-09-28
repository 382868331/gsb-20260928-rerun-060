"""差分隐私预算账（epsilon 预算）。

预算合成规则：

* 本系统第一版只注册“计数”类机制，全部采用 (epsilon, delta=0) 的纯 DP
  拉普拉斯机制，因此多轮发布按**线性组合 / 串行组合定理**合成：
  ``epsilon_total = sum(epsilon_i)``。
* 不使用更松或更紧的高级组合（advanced composition）：那需要固定轮数并
  引入 delta，与纯 DP 账不符。
* 全局只有一个预算池（第一版不区分 per-key 子池），单位为 epsilon，
  初始总额在开户时设定且不可上调。

记账与发布在同一个 SQLite 事务内完成（见 :mod:`privacy_stats.publisher`），
保证“发布结果和扣除预算一起成功”；请求幂等键使重启后重放同一请求时
返回既有结果且**不重复扣费、不重复发噪**。
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass


class BudgetExhaustedError(RuntimeError):
    """剩余预算不足以支付本次查询。"""

    def __init__(self, required: float, remaining: float):
        self.required = required
        self.remaining = remaining
        super().__init__(
            f"预算不足：本次需要 epsilon={required:.6g}，仅剩 {remaining:.6g}"
        )


@dataclass(frozen=True)
class BudgetStatus:
    total: float
    spent: float

    @property
    def remaining(self) -> float:
        return self.total - self.spent


class BudgetLedger:
    """SQLite 预算账。

    表 ``budget_account`` 只有单行（id=1）。扣费采用条件 UPDATE 做
    compare-and-deduct（``WHERE remaining_e >= ?``），配合事务保证并发/
    重放下不会超支。
    """

    def __init__(self, conn: sqlite3.Connection, total_epsilon: float):
        if total_epsilon <= 0:
            raise ValueError("总预算必须为正数")
        self._conn = conn
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS budget_account (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                total_e     REAL NOT NULL,
                remaining_e REAL NOT NULL,
                opened_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS budget_entries (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id        TEXT NOT NULL UNIQUE,
                version_id        TEXT NOT NULL,
                mechanism         TEXT NOT NULL,
                epsilon_spent     REAL NOT NULL,
                remaining_after   REAL NOT NULL,
                created_at        TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """
        )
        self._conn.execute(
            """
            INSERT OR IGNORE INTO budget_account (id, total_e, remaining_e)
            VALUES (1, ?, ?)
            """,
            (float(total_epsilon), float(total_epsilon)),
        )
        self._conn.commit()

    def status(self) -> BudgetStatus:
        total, remaining = self._conn.execute(
            "SELECT total_e, remaining_e FROM budget_account WHERE id = 1"
        ).fetchone()
        return BudgetStatus(total=float(total), spent=float(total - remaining))

    def has_request(self, request_id: str) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM budget_entries WHERE request_id = ?", (request_id,)
        ).fetchone()
        return row is not None

    def spend(
        self,
        request_id: str,
        version_id: str,
        mechanism: str,
        epsilon: float,
    ) -> float:
        """在调用方事务内尝试扣费，返回扣费后剩余预算。

        必须由 publisher 在写发布结果的同一事务中调用。
        预算不足时抛 :class:`BudgetExhaustedError`，不产生任何写入。
        """
        if epsilon <= 0:
            raise ValueError("epsilon 必须为正数")
        cur = self._conn.execute(
            """
            UPDATE budget_account
               SET remaining_e = remaining_e - ?
             WHERE id = 1 AND remaining_e >= ?
            """,
            (float(epsilon), float(epsilon)),
        )
        if cur.rowcount == 0:
            remaining = self.status().remaining
            raise BudgetExhaustedError(required=epsilon, remaining=remaining)
        remaining_after = self._conn.execute(
            "SELECT remaining_e FROM budget_account WHERE id = 1"
        ).fetchone()[0]
        self._conn.execute(
            """
            INSERT INTO budget_entries
                (request_id, version_id, mechanism, epsilon_spent, remaining_after)
            VALUES (?, ?, ?, ?, ?)
            """,
            (request_id, version_id, mechanism, float(epsilon), float(remaining_after)),
        )
        return float(remaining_after)
