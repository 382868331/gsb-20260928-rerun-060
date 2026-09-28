"""隐私发布系统的确定性测试（标准库 unittest，无第三方依赖）。"""

from __future__ import annotations

import json
import math
import os
import random
import tempfile
import unittest

from privacy_system import PrivacyGateway, BudgetExhausted, DuplicateRequest, UnknownVersion
from privacy_system.catalog import OPERATIONS
from privacy_system.fictional_data import FICTIONAL_ROWS
from privacy_system.mechanism import laplace_noise


class MechanismTest(unittest.TestCase):
    def test_laplace_distribution_mean_scale(self) -> None:
        rng = random.Random(42)
        epsilon = 0.5
        samples = [laplace_noise(rng, 1.0, epsilon) for _ in range(200_000)]
        mean = sum(samples) / len(samples)
        self.assertLess(abs(mean), 0.02)
        mean_abs = sum(abs(x) for x in samples) / len(samples)
        self.assertLess(abs(mean_abs - 1.0 / epsilon), 0.02)

    def test_fixed_seed_is_deterministic(self) -> None:
        a = [laplace_noise(random.Random(123), 1.0, 1.0) for _ in range(5)]
        b = [laplace_noise(random.Random(123), 1.0, 1.0) for _ in range(5)]
        self.assertEqual(a, b)


class GatewayTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "ledger.json")
        self.seed = 20260929

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _gateway(self, total: float = 10.0) -> PrivacyGateway:
        gw = PrivacyGateway(self.db_path, total_epsilon=total, rng=random.Random(self.seed))
        self.addCleanup(gw.close)
        gw.register_fictional_v1()
        return gw

    def test_fixed_rng_release_and_ledger_bookkeeping(self) -> None:
        gw = self._gateway(total=10.0)
        expected_noise = laplace_noise(random.Random(self.seed), 1.0, 1.0)
        result = gw.release("r-1", "fictional-v1", "count_all", {}, epsilon=1.0)
        self.assertEqual(len(FICTIONAL_ROWS), 10)
        self.assertTrue(math.isclose(result.noise, expected_noise, rel_tol=0, abs_tol=1e-12))
        self.assertTrue(math.isclose(result.value, 10 + expected_noise, rel_tol=0, abs_tol=1e-12))
        self.assertFalse(result.is_replay)
        self.assertEqual(result.mechanism, "laplace")
        self.assertEqual(result.delta, 0.0)
        budget = gw.budget()
        self.assertTrue(math.isclose(budget["spent_epsilon"], 1.0))
        self.assertTrue(math.isclose(budget["remaining_epsilon"], 9.0))

        # 直接核对磁盘账本，发布记录与扣账必须同时落库。
        with open(self.db_path, encoding="utf-8") as handle:
            persisted = json.load(handle)
        self.assertEqual(persisted["spent_epsilon"], 1.0)
        self.assertEqual(len(persisted["releases"]), 1)

    def test_duplicate_request_replays_without_recharge(self) -> None:
        gw = self._gateway()
        first = gw.release("dup", "fictional-v1", "count_by_city", {"city": "Beijing"}, epsilon=1.5)
        second = gw.release("dup", "fictional-v1", "count_by_city", {"city": "Beijing"}, epsilon=1.5)
        self.assertFalse(first.is_replay)
        self.assertTrue(second.is_replay)
        self.assertEqual(first.value, second.value)
        self.assertEqual(first.noise, second.noise)
        self.assertEqual(second.epsilon_spent, 0.0)
        self.assertTrue(math.isclose(gw.budget()["spent_epsilon"], 1.5))

        with self.assertRaises(DuplicateRequest):
            gw.release("dup", "fictional-v1", "count_by_city", {"city": "Shanghai"})

    def test_same_query_new_id_is_new_charge(self) -> None:
        gw = self._gateway()
        r1 = gw.release("a", "fictional-v1", "count_all", {}, epsilon=1.0)
        r2 = gw.release("b", "fictional-v1", "count_all", {}, epsilon=1.0)
        self.assertFalse(r1.is_replay)
        self.assertFalse(r2.is_replay)
        self.assertNotEqual(r1.noise, r2.noise)  # 独立加噪
        self.assertTrue(math.isclose(gw.budget()["spent_epsilon"], 2.0))

    def test_budget_exhausted_rejects_and_publishes_nothing(self) -> None:
        gw = self._gateway(total=2.0)
        gw.release("small", "fictional-v1", "count_all", {}, epsilon=1.0)
        with self.assertRaises(BudgetExhausted):
            gw.release("too-big", "fictional-v1", "count_all", {}, epsilon=1.5)
        budget = gw.budget()
        self.assertTrue(math.isclose(budget["spent_epsilon"], 1.0))
        self.assertTrue(math.isclose(budget["remaining_epsilon"], 1.0))
        ids = {row["request_id"] for row in gw.list_releases()}
        self.assertEqual(ids, {"small"})

        # 恰好等于剩余预算可以成功；之后任何正 epsilon 请求都被拒绝。
        gw.release("exact", "fictional-v1", "count_all", {}, epsilon=1.0)
        with self.assertRaises(BudgetExhausted):
            gw.release("after-zero", "fictional-v1", "count_all", {}, epsilon=0.01)
        self.assertTrue(math.isclose(gw.budget()["remaining_epsilon"], 0.0))

    def test_restart_no_double_spend(self) -> None:
        gw = self._gateway(total=3.0)
        first = gw.release("persist", "fictional-v1", "count_all", {}, epsilon=1.0)
        gw.close()

        # 模拟进程重启：新网关、新随机源，重新登记同一版本数据。
        gw2 = PrivacyGateway(self.db_path, total_epsilon=3.0, rng=random.Random(999))
        self.addCleanup(gw2.close)
        gw2.register_fictional_v1()
        budget = gw2.budget()
        self.assertTrue(math.isclose(budget["spent_epsilon"], 1.0))
        replay = gw2.release("persist", "fictional-v1", "count_all", {}, epsilon=1.0)
        self.assertTrue(replay.is_replay)
        self.assertEqual(replay.value, first.value)
        self.assertEqual(replay.noise, first.noise)
        self.assertTrue(math.isclose(gw2.budget()["spent_epsilon"], 1.0))

        with self.assertRaises(BudgetExhausted):
            gw2.release("fresh", "fictional-v1", "count_all", {}, epsilon=2.5)
        gw2.release("fits", "fictional-v1", "count_all", {}, epsilon=2.0)

    def test_versions_and_unknown_version(self) -> None:
        gw = self._gateway()
        with self.assertRaises(UnknownVersion):
            gw.release("x", "nope", "count_all", {}, epsilon=1.0)
        versions = {v["version"] for v in gw.list_versions()}
        self.assertIn("fictional-v1", versions)

    def test_only_registered_count_ops_allowed(self) -> None:
        gw = self._gateway()
        with self.assertRaises(Exception):
            gw.release("bad", "fictional-v1", "sum_salary", {}, epsilon=1.0)
        with self.assertRaises(ValueError):
            gw.release("bad2", "fictional-v1", "count_by_city", {"city": "Beijing", "id": "1"})
        self.assertEqual(set(OPERATIONS), {"count_all", "count_by_city", "count_by_age_group", "count_by_membership"})

    def test_released_records_never_expose_true_count(self) -> None:
        gw = self._gateway()
        gw.release("pub", "fictional-v1", "count_by_city", {"city": "Beijing"}, epsilon=1.0)
        for row in gw.list_releases():
            serialized = json.dumps(row, ensure_ascii=False)
            self.assertNotIn("true_count", serialized)


if __name__ == "__main__":
    unittest.main()
