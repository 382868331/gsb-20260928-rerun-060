"""多统计批次、事实复用、取消、争用与对账测试。"""

from __future__ import annotations

import json
import math
import os
import random
import tempfile
import threading
import unittest

from privacy_system import BudgetExhausted, DuplicateRequest, PrivacyGateway, ReleaseInterrupted


class BatchTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "ledger.json")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _gateway(self, total: float = 10.0, seed: int = 31) -> PrivacyGateway:
        gw = PrivacyGateway(self.db_path, total_epsilon=total, rng=random.Random(seed))
        self.addCleanup(gw.close)
        gw.register_fictional_v1()
        return gw

    @staticmethod
    def _items():
        return [
            {"item_id": "all", "operation": "count_all", "params": {}, "epsilon": 1.0},
            {
                "item_id": "beijing",
                "operation": "count_by_city",
                "params": {"city": "Beijing"},
                "epsilon": 1.5,
            },
            {
                "item_id": "members",
                "operation": "count_by_membership",
                "params": {"membership": "yes"},
                "epsilon": 1.0,
            },
        ]

    def test_all_or_nothing_batch_publishes_together_and_replays(self) -> None:
        gw = self._gateway(total=4.0)
        first = gw.release_batch("b-all", "fictional-v1", self._items(), mode="all")
        self.assertEqual(
            [item["status"] for item in first["items"]],
            ["published", "published", "published"],
        )
        self.assertFalse(first["is_replay"])
        self.assertTrue(math.isclose(gw.budget()["spent_epsilon"], 3.5))

        second = gw.release_batch("b-all", "fictional-v1", self._items(), mode="all")
        self.assertTrue(second["is_replay"])
        self.assertEqual(
            [item["noisy_value"] for item in first["items"]],
            [item["noisy_value"] for item in second["items"]],
        )
        self.assertTrue(math.isclose(gw.budget()["spent_epsilon"], 3.5))

    def test_all_or_nothing_insufficient_budget_leaks_nothing(self) -> None:
        gw = self._gateway(total=2.0)
        with self.assertRaises(BudgetExhausted):
            gw.release_batch("b-fail", "fictional-v1", self._items(), mode="all")
        self.assertTrue(math.isclose(gw.budget()["spent_epsilon"], 0.0))
        self.assertEqual(gw.batch_status("b-fail")["status"], "not_released")
        self.assertEqual(gw.list_releases(), [])
        self.assertEqual(gw.reconcile_batch("b-fail")["status"], "not_released")

    def test_partial_batch_reports_each_item_and_budget(self) -> None:
        gw = self._gateway(total=2.0)
        receipt = gw.release_batch("b-part", "fictional-v1", self._items(), mode="partial")
        statuses = [item["status"] for item in receipt["items"]]
        self.assertEqual(statuses, ["published", "not_published", "published"])
        self.assertIsNotNone(receipt["items"][0]["noisy_value"])
        self.assertIsNone(receipt["items"][1]["noisy_value"])
        self.assertIsNotNone(receipt["items"][2]["noisy_value"])
        self.assertEqual(
            [item["epsilon_spent"] for item in receipt["items"]], [1.0, 0.0, 1.0]
        )
        self.assertTrue(math.isclose(gw.budget()["spent_epsilon"], 2.0))

        cancelled = gw.cancel_batch_items("b-part", ["beijing"])
        self.assertEqual(cancelled["items"][1]["status"], "cancelled")
        replay = gw.release_batch("b-part", "fictional-v1", self._items(), mode="partial")
        self.assertEqual(
            replay["items"][0]["noisy_value"], receipt["items"][0]["noisy_value"]
        )
        self.assertEqual(replay["items"][1]["status"], "cancelled")
        self.assertTrue(math.isclose(gw.budget()["spent_epsilon"], 2.0))

    def test_reuse_requires_same_release_fact_not_display_name(self) -> None:
        gw = self._gateway(total=10.0)
        source = gw.release(
            "src", "fictional-v1", "count_by_city", {"city": "Beijing"}, epsilon=1.0
        )
        matching = [
            {
                "item_id": "same-filter",
                "operation": "count_by_city",
                "params": {"city": "Beijing"},
                "epsilon": 9.0,
                "source_request_id": "src",
            }
        ]
        receipt = gw.release_batch("b-reuse", "fictional-v1", matching, mode="partial")
        self.assertEqual(receipt["items"][0]["status"], "reused")
        self.assertEqual(receipt["items"][0]["noisy_value"], source.value)
        self.assertEqual(receipt["items"][0]["epsilon_spent"], 0.0)
        self.assertTrue(math.isclose(gw.budget()["spent_epsilon"], 1.0))

        changed = [
            {
                "item_id": "other-filter",
                "operation": "count_by_city",
                "params": {"city": "Shanghai"},
                "epsilon": 9.0,
                "source_request_id": "src",
            }
        ]
        with self.assertRaises(DuplicateRequest):
            gw.release_batch("b-wrong-reuse", "fictional-v1", changed, mode="partial")

    def test_interrupted_all_batch_claim_is_stable_and_no_recharge(self) -> None:
        gw = self._gateway(total=4.0, seed=77)
        gw.fail_next_delivery("b-crash")
        with self.assertRaises(ReleaseInterrupted):
            gw.release_batch("b-crash", "fictional-v1", self._items(), mode="all")
        self.assertTrue(math.isclose(gw.budget()["spent_epsilon"], 3.5))
        for item_id in ("all", "beijing", "members"):
            rid = f"batch:b-crash:{item_id}"
            self.assertEqual(
                gw.request_status(rid)["status"], "published_unacknowledged"
            )

        claimed = gw.claim_batch("b-crash")
        again = gw.claim_batch("b-crash")
        self.assertTrue(again["is_replay"])
        self.assertEqual(
            [item["noisy_value"] for item in claimed["items"]],
            [item["noisy_value"] for item in again["items"]],
        )
        self.assertTrue(math.isclose(gw.budget()["spent_epsilon"], 3.5))

    def test_overlapping_partial_batches_contend_without_overspend(self) -> None:
        gw = self._gateway(total=5.0, seed=44)
        items = [
            {"item_id": "a", "operation": "count_all", "params": {}, "epsilon": 2.0},
            {"item_id": "b", "operation": "count_all", "params": {}, "epsilon": 2.0},
            {"item_id": "c", "operation": "count_all", "params": {}, "epsilon": 2.0},
        ]
        barrier = threading.Barrier(2)
        errors = []

        def submit(batch_id: str) -> None:
            barrier.wait()
            try:
                gw.release_batch(batch_id, "fictional-v1", items, mode="partial")
            except Exception as exc:  # pragma: no cover - 测试失败时输出
                errors.append(exc)

        threads = [threading.Thread(target=submit, args=(f"overlap-{i}",)) for i in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        published = 0
        for batch_id in ("overlap-0", "overlap-1"):
            published += sum(
                item["status"] == "published"
                for item in gw.reconcile_batch(batch_id)["items"]
            )
        self.assertEqual(published, 2)
        self.assertTrue(math.isclose(gw.budget()["spent_epsilon"], 4.0))

    def test_reconciliation_exposes_identities_and_budget_only(self) -> None:
        gw = self._gateway(total=2.0)
        gw.release_batch("b-pub", "fictional-v1", self._items(), mode="partial")
        gw.cancel_batch_items("b-pub", ["beijing"])
        payload = json.dumps(gw.reconcile_batch("b-pub"), ensure_ascii=False)
        self.assertNotIn("noisy_value", payload)
        self.assertNotIn("true_count", payload)
        self.assertNotIn("noise", payload)
        report = gw.reconcile_batch("b-pub")
        self.assertEqual(report["new_epsilon_spent"], 2.0)
        self.assertEqual(report["cancelled_count"], 1)
        self.assertEqual(
            report["items"][0]["release_request_id"], "batch:b-pub:all"
        )


if __name__ == "__main__":
    unittest.main()
