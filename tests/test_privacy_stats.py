"""隐私统计发布系统的记账与发布语义测试（标准库 unittest，零依赖）。

全部使用固定种子随机源（DeterministicRandomSource），
因此带噪结果与逐笔预算余额都可以逐值核对。
"""

from __future__ import annotations

import math
import random
import tempfile
import unittest
from pathlib import Path

from privacy_stats import (
    BudgetExhaustedError,
    BudgetLedger,
    Publisher,
    DataVersionStore,
)
from privacy_stats.db import open_connection
from privacy_stats.mechanisms import REGISTRY
from privacy_stats.noise import DeterministicRandomSource
from privacy_stats.versions import DataVault


FICTIVE_COLUMNS = ("id", "city", "age_group")
FICTIVE_ROWS = [
    (1, "北京", "20-29"),
    (2, "北京", "30-39"),
    (3, "上海", "20-29"),
    (4, "上海", "20-29"),
    (5, "广州", "40-49"),
    (6, "北京", "30-39"),
    (7, "上海", "30-39"),
    (8, "广州", "20-29"),
]
TRUE_TOTAL = 8
SEED = 20260929


def expected_laplace(seed: int, index: int) -> float:
    """独立复算 DeterministicRandomSource 的第 index 个 Laplace 值。"""
    rng = random.Random(seed)
    value = 0.0
    for _ in range(index + 1):
        u = rng.random() - 0.5
        sign = 1.0 if u >= 0 else -1.0
        value = -1.0 * sign * math.log(1.0 - 2.0 * abs(u))
    return value


class PrivacyStatsTestCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.conn = open_connection(str(self.tmp / "stats.db"))
        self.versions = DataVersionStore(self.conn)
        self.ledger = BudgetLedger(self.conn, 3.0)
        self.publisher = Publisher(
            self.conn,
            self.versions,
            self.ledger,
            DeterministicRandomSource(SEED),
        )
        self.version = self.versions.create_version(
            "fictive-people-v1", FICTIVE_COLUMNS, FICTIVE_ROWS
        )

    def tearDown(self):
        self.conn.close()
        for extra in ("_tmp2", "_tmp3"):
            tmp = getattr(self, extra, None)
            if tmp is not None:
                tmp.cleanup()
        self._tmp.cleanup()

    def test_first_release_matches_fixed_rng_and_charges_epsilon(self):
        result = self.publisher.publish(
            "req-1", self.version.version_id, "total_count"
        )
        expected_raw = TRUE_TOTAL + expected_laplace(SEED, 0)
        self.assertFalse(result.reused)
        self.assertAlmostEqual(result.noisy_value_raw, expected_raw, places=12)
        self.assertEqual(result.noisy_result, round(expected_raw))
        self.assertAlmostEqual(result.epsilon_spent, 1.0)
        self.assertAlmostEqual(result.budget_remaining, 2.0)
        self.assertAlmostEqual(self.ledger.status().spent, 1.0)
        self.assertIn("不是真实精确数量", result.privacy_note)

    def test_duplicate_request_returns_same_release_without_second_charge(self):
        first = self.publisher.publish(
            "req-dup", self.version.version_id, "total_count"
        )
        second = self.publisher.publish(
            "req-dup", self.version.version_id, "total_count"
        )
        third = self.publisher.publish(
            "req-dup", self.version.version_id, "total_count", params={}
        )
        for reused in (second, third):
            self.assertTrue(reused.reused)
            self.assertEqual(reused.noisy_result, first.noisy_result)
            self.assertEqual(reused.noisy_value_raw, first.noisy_value_raw)
            self.assertEqual(reused.epsilon_spent, 0.0)
            self.assertAlmostEqual(reused.budget_remaining, 2.0)
        self.assertAlmostEqual(self.ledger.status().spent, 1.0)
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) FROM budget_entries").fetchone()[0],
            1,
        )
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) FROM releases").fetchone()[0], 1
        )

    def test_replayed_after_restart_is_not_reissued(self):
        first = self.publisher.publish(
            "req-persist", self.version.version_id, "total_count"
        )
        db_file = self.tmp / "stats.db"
        self.conn.close()

        conn2 = open_connection(str(db_file))
        versions2 = DataVersionStore(conn2)
        ledger2 = BudgetLedger(conn2, 3.0)
        publisher2 = Publisher(
            conn2, versions2, ledger2, DeterministicRandomSource(SEED)
        )

        replay = publisher2.publish(
            "req-persist", self.version.version_id, "total_count"
        )
        self.assertTrue(replay.reused)
        self.assertEqual(replay.noisy_result, first.noisy_result)
        self.assertEqual(replay.epsilon_spent, 0.0)
        self.assertAlmostEqual(ledger2.status().spent, 1.0)

        new_result = publisher2.publish(
            "req-after-restart", self.version.version_id, "total_count"
        )
        self.assertFalse(new_result.reused)
        expected = TRUE_TOTAL + expected_laplace(SEED, 0)
        self.assertAlmostEqual(new_result.noisy_value_raw, expected, places=12)
        self.assertAlmostEqual(new_result.budget_remaining, 1.0)
        conn2.close()

    def test_budget_exhaustion_rejects_without_release_or_charge(self):
        # 重建一个总额 2.0 的账
        self.conn.close()
        self._tmp2 = tempfile.TemporaryDirectory()
        db = Path(self._tmp2.name) / "s.db"
        self.conn = open_connection(str(db))
        self.versions = DataVersionStore(self.conn)
        self.ledger = BudgetLedger(self.conn, 2.0)
        self.publisher = Publisher(
            self.conn, self.versions, self.ledger, DeterministicRandomSource(SEED)
        )
        self.version = self.versions.create_version(
            "v", FICTIVE_COLUMNS, FICTIVE_ROWS
        )

        self.publisher.publish("req-a", self.version.version_id, "total_count")
        self.publisher.publish(
            "req-b",
            self.version.version_id,
            "predicate_count",
            {"column": "city", "value": "北京"},
        )
        self.assertAlmostEqual(self.ledger.status().remaining, 0.0)

        with self.assertRaises(BudgetExhaustedError) as ctx:
            self.publisher.publish("req-c", self.version.version_id, "total_count")
        self.assertAlmostEqual(ctx.exception.required, 1.0)
        self.assertAlmostEqual(ctx.exception.remaining, 0.0)

        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) FROM releases").fetchone()[0], 2
        )
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) FROM budget_entries").fetchone()[0],
            2,
        )
        self.assertAlmostEqual(self.ledger.status().spent, 2.0)

    def test_partial_budget_rejects_fractional_overspend(self):
        self.conn.close()
        self._tmp3 = tempfile.TemporaryDirectory()
        db = Path(self._tmp3.name) / "s.db"
        self.conn = open_connection(str(db))
        self.versions = DataVersionStore(self.conn)
        self.ledger = BudgetLedger(self.conn, 1.5)
        self.publisher = Publisher(
            self.conn, self.versions, self.ledger, DeterministicRandomSource(SEED)
        )
        self.version = self.versions.create_version(
            "v", FICTIVE_COLUMNS, FICTIVE_ROWS
        )
        self.publisher.publish("req-a", self.version.version_id, "total_count")
        self.assertAlmostEqual(self.ledger.status().remaining, 0.5)
        with self.assertRaises(BudgetExhaustedError):
            self.publisher.publish("req-b", self.version.version_id, "total_count")
        self.assertAlmostEqual(self.ledger.status().remaining, 0.5)

    def test_same_request_id_with_different_query_is_rejected(self):
        self.publisher.publish(
            "req-x", self.version.version_id, "total_count"
        )
        with self.assertRaisesRegex(ValueError, "已绑定其他查询"):
            self.publisher.publish(
                "req-x",
                self.version.version_id,
                "predicate_count",
                {"column": "city", "value": "北京"},
            )
        self.assertAlmostEqual(self.ledger.status().spent, 1.0)

    def test_new_query_on_new_version_is_distinct_and_charged(self):
        v2 = self.versions.create_version(
            "fictive-people-v2",
            FICTIVE_COLUMNS,
            FICTIVE_ROWS + [(9, "深圳", "20-29")],
        )
        self.assertNotEqual(v2.version_id, self.version.version_id)
        self.publisher.publish(
            "req-v1", self.version.version_id, "total_count"
        )
        r2 = self.publisher.publish("req-v2", v2.version_id, "total_count")
        self.assertFalse(r2.reused)
        self.assertAlmostEqual(
            r2.noisy_value_raw, 9 + expected_laplace(SEED, 1), places=12
        )
        self.assertAlmostEqual(self.ledger.status().spent, 2.0)

    def test_only_registered_mechanisms_are_publishable(self):
        self.assertEqual(set(REGISTRY), {"total_count", "predicate_count"})
        with self.assertRaisesRegex(KeyError, "未注册的机制"):
            self.publisher.publish(
                "req-bad", self.version.version_id, "sum_salary"
            )
        self.assertAlmostEqual(self.ledger.status().spent, 0.0)

    def test_raw_rows_only_accessible_through_vault(self):
        self.assertFalse(hasattr(self.publisher, "rows_of"))
        self.assertFalse(hasattr(self.ledger, "rows_of"))
        vault = DataVault(self.versions)
        self.assertEqual(len(vault.rows_of(self.version.version_id)), TRUE_TOTAL)

    def test_invalid_params_fail_before_any_charge(self):
        with self.assertRaisesRegex(KeyError, "列不存在"):
            self.publisher.publish(
                "req-bad-col",
                self.version.version_id,
                "predicate_count",
                {"column": "nonexistent", "value": "x"},
            )
        self.assertAlmostEqual(self.ledger.status().spent, 0.0)
        ok = self.publisher.publish(
            "req-ok", self.version.version_id, "total_count"
        )
        self.assertAlmostEqual(
            ok.noisy_value_raw,
            TRUE_TOTAL + expected_laplace(SEED, 0),
            places=12,
        )

    def test_identical_data_dedupes_to_same_version_id(self):
        again = self.versions.create_version(
            "different-name-same-data", FICTIVE_COLUMNS, FICTIVE_ROWS
        )
        self.assertEqual(again.version_id, self.version.version_id)


if __name__ == "__main__":
    unittest.main()
