"""端到端演示：建版本 -> 发布计数 -> 重复请求 -> 预算耗尽。

运行：python3 examples/run_demo.py
使用固定种子随机源，输出可复现。
"""

from __future__ import annotations

import tempfile
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from privacy_stats import BudgetExhaustedError, BudgetLedger, Publisher, DataVersionStore
from privacy_stats.db import open_connection
from privacy_stats.noise import DeterministicRandomSource


PEOPLE_COLUMNS = ("id", "city")
PEOPLE_ROWS = [
    (1, "北京"), (2, "北京"), (3, "上海"),
    (4, "上海"), (5, "上海"), (6, "广州"),
]


def main() -> None:
    tmp = tempfile.TemporaryDirectory()
    conn = open_connection(str(Path(tmp.name) / "demo.db"))
    versions = DataVersionStore(conn)
    ledger = BudgetLedger(conn, total_epsilon=2.0)
    publisher = Publisher(conn, versions, ledger, DeterministicRandomSource(42))

    v1 = versions.create_version("fictive-v1", PEOPLE_COLUMNS, PEOPLE_ROWS)

    r1 = publisher.publish("demo-total", v1.version_id, "total_count")
    print(
        f"[首次] {r1.mechanism}: 带噪结果={r1.noisy_result} "
        f"(原始带噪值={r1.noisy_value_raw:.4f}), "
        f"扣费={r1.epsilon_spent}, 剩余预算={r1.budget_remaining}"
    )

    r_dup = publisher.publish("demo-total", v1.version_id, "total_count")
    print(
        f"[重复] reused={r_dup.reused}, 扣费={r_dup.epsilon_spent}, "
        f"结果一致={r_dup.noisy_result == r1.noisy_result}"
    )

    r2 = publisher.publish(
        "demo-bj",
        v1.version_id,
        "predicate_count",
        {"column": "city", "value": "北京"},
    )
    print(
        f"[新查询] {r2.params}: 带噪结果={r2.noisy_result}, "
        f"剩余预算={r2.budget_remaining}"
    )

    try:
        publisher.publish("demo-denied", v1.version_id, "total_count")
    except BudgetExhaustedError as exc:
        print(f"[拒绝] {exc}")

    print(f"[声明] {r1.privacy_note}")
    print(
        f"[账] total={ledger.status().total}, spent={ledger.status().spent}, "
        f"remaining={ledger.status().remaining}"
    )

    conn.close()
    tmp.cleanup()


if __name__ == "__main__":
    main()
