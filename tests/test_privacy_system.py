"""隐私发布系统的确定性测试（标准库 unittest，无第三方依赖）。"""

from __future__ import annotations

import json
import math
import os
import random
import subprocess
import sys
import tempfile
import threading
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
        self.assertTrue(math.isclose(result.value, 10 + expected_noise, rel_tol=0, abs_tol=1e-12))
        self.assertFalse(hasattr(result, "noise"))  # 公开回执不得携带噪声
        self.assertFalse(result.is_replay)
        self.assertEqual(result.mechanism, "laplace")
        self.assertEqual(result.delta, 0.0)
        budget = gw.budget()
        self.assertTrue(math.isclose(budget["spent_epsilon"], 1.0))
        self.assertTrue(math.isclose(budget["remaining_epsilon"], 9.0))

        # 直接核对磁盘账本，发布记录与扣账必须同时落库；
        # 真实计数与噪声只留在内部账本中供审计。
        with open(self.db_path, encoding="utf-8") as handle:
            persisted = json.load(handle)
        self.assertEqual(persisted["spent_epsilon"], 1.0)
        self.assertEqual(len(persisted["releases"]), 1)
        record = persisted["releases"]["r-1"]
        self.assertEqual(record["true_count"], 10)
        self.assertTrue(math.isclose(record["noise"], expected_noise, rel_tol=0, abs_tol=1e-12))
        audit = gw.audit_release("r-1")
        self.assertEqual(audit["true_count"], 10)
        self.assertTrue(math.isclose(audit["noise"], expected_noise, rel_tol=0, abs_tol=1e-12))

    def test_duplicate_request_replays_without_recharge(self) -> None:
        gw = self._gateway()
        first = gw.release("dup", "fictional-v1", "count_by_city", {"city": "Beijing"}, epsilon=1.5)
        second = gw.release("dup", "fictional-v1", "count_by_city", {"city": "Beijing"}, epsilon=1.5)
        self.assertFalse(first.is_replay)
        self.assertTrue(second.is_replay)
        self.assertEqual(first.value, second.value)
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
        self.assertNotEqual(r1.value, r2.value)  # 独立加噪
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
            self.assertNotIn("noise", serialized)


class ConcurrencyTest(unittest.TestCase):
    """并发重复请求与并发新请求的一致性。"""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "ledger.json")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_concurrent_duplicate_requests_charge_once(self) -> None:
        gw = PrivacyGateway(self.db_path, total_epsilon=10.0, rng=random.Random(7))
        self.addCleanup(gw.close)
        gw.register_fictional_v1()

        results = []
        errors = []

        def worker() -> None:
            try:
                results.append(
                    gw.release("race", "fictional-v1", "count_all", {}, epsilon=1.0)
                )
            except Exception as exc:  # pragma: no cover - 失败时记录
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(16)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(errors, [])
        self.assertEqual(len(results), 16)
        # 全部请求拿到同一份公开结果，且预算只扣一次。
        self.assertEqual(len({r.value for r in results}), 1)
        self.assertEqual(sum(1 for r in results if not r.is_replay), 1)
        self.assertTrue(math.isclose(gw.budget()["spent_epsilon"], 1.0))
        self.assertEqual(len(gw.list_releases()), 1)

    def test_concurrent_distinct_requests_never_overspend(self) -> None:
        gw = PrivacyGateway(self.db_path, total_epsilon=3.0, rng=random.Random(11))
        self.addCleanup(gw.close)
        gw.register_fictional_v1()

        outcomes = []
        lock = threading.Lock()

        def worker(index: int) -> None:
            try:
                gw.release(f"c-{index}", "fictional-v1", "count_all", {}, epsilon=1.0)
                outcome = "ok"
            except Exception:
                outcome = "rejected"
            with lock:
                outcomes.append(outcome)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        # 预算 3.0、每次 1.0：恰好 3 次成功，其余拒绝，绝不超支。
        self.assertEqual(outcomes.count("ok"), 3)
        self.assertEqual(outcomes.count("rejected"), 7)
        self.assertTrue(math.isclose(gw.budget()["spent_epsilon"], 3.0))
        with open(self.db_path, encoding="utf-8") as handle:
            persisted = json.load(handle)
        self.assertEqual(len(persisted["releases"]), 3)
        self.assertTrue(math.isclose(persisted["spent_epsilon"], 3.0))


class PublicSurfaceTest(unittest.TestCase):
    """只用公开资料核对：无法直接还原真实计数（不是只查字段名）。"""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "ledger.json")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    @staticmethod
    def _numbers(payload) -> list[float]:
        """递归收集公开负载里的所有数值型字段（不解析字符串）。

        算术还原只能利用数值字段；字符串（时间戳、ID 等）中的数字
        不是可参与运算的公开数值，故不纳入组合。
        """
        found = []
        if isinstance(payload, bool):
            return found
        if isinstance(payload, (int, float)):
            found.append(float(payload))
        elif isinstance(payload, dict):
            for value in payload.values():
                found.extend(PublicSurfaceTest._numbers(value))
        elif isinstance(payload, (list, tuple)):
            for item in payload:
                found.extend(PublicSurfaceTest._numbers(item))
        return found

    def test_public_outputs_cannot_reconstruct_true_counts(self) -> None:
        # 预算与 epsilon 取非整数，避免与计数巧合相等造成误判。
        gw = PrivacyGateway(self.db_path, total_epsilon=10.5, rng=random.Random(20260930))
        self.addCleanup(gw.close)
        version = gw.register_fictional_v1()
        queries = [
            ("p-1", "count_all", {}),
            ("p-2", "count_by_city", {"city": "Beijing"}),
            ("p-3", "count_by_age_group", {"age_group": "20-29"}),
            ("p-4", "count_by_membership", {"membership": "yes"}),
        ]
        results = [
            gw.release(rid, version, op, params, epsilon=1.25)
            for rid, op, params in queries
        ]
        # 触发一条失败信息，失败文本也属于公开面。
        try:
            gw.release("p-5", version, "count_all", {}, epsilon=99.5)
        except Exception as exc:
            error_text = str(exc)

        # 汇总查询者能拿到的一切公开资料。
        public_payloads = [r.to_dict() for r in results]
        public_payloads.extend(gw.list_releases())
        public_payloads.append(gw.budget())
        public_payloads.extend(gw.list_versions())
        public_payloads.append(error_text)
        cli = subprocess.run(
            [sys.executable, "-m", "privacy_system.cli",
             "--db", os.path.join(self.tmp.name, "cli.json"),
             "--total-epsilon", "10.5", "--seed", "5"],
            capture_output=True, text=True, check=True,
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        )
        public_payloads.append(cli.stdout)
        self.assertNotIn("噪声", cli.stdout)

        # 公开资料中不得出现“噪声/真实计数”类字段。
        for payload in public_payloads:
            serialized = payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False)
            self.assertNotIn("true_count", serialized)
            self.assertNotIn("noise", serialized)

        # 对每条发布，用其公开数值的各种组合实际尝试还原真实计数：
        # 单个公开数、两数之和/之差（覆盖“带噪值 − 噪声”这类还原手法）。
        # 若噪声仍随公开结果一起给出，value - noise 必然命中真实计数。
        listed = {row["request_id"]: row for row in gw.list_releases()}
        for result, (rid, operation, params) in zip(results, queries):
            op = OPERATIONS[operation]
            true_count = op.compute(
                (None, tuple(dict(row) for row in FICTIONAL_ROWS)), params
            )
            numbers = self._numbers(result.to_dict()) + self._numbers(listed[rid])
            candidates = set(numbers)
            for a in numbers:
                for b in numbers:
                    candidates.add(a + b)
                    candidates.add(a - b)
            for candidate in candidates:
                self.assertFalse(
                    math.isclose(candidate, true_count, rel_tol=0, abs_tol=1e-9),
                    f"公开资料组合 {candidate} 还原了 {operation}{params} 的真实计数 {true_count}",
                )


if __name__ == "__main__":
    unittest.main()
