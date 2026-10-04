"""发布安排的预演、批准、分阶段兑现与重启一致性测试。"""

from __future__ import annotations

import json
import math
import os
import random
import tempfile
import unittest

from privacy_system import BudgetExhausted, DuplicateRequest, PrivacyGateway
from privacy_system.gateway import MODE_ALL, MODE_PHASED


class ArrangementTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "ledger.json")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _gateway(self, total: float = 3.0, seed: int = 1234) -> PrivacyGateway:
        gw = PrivacyGateway(self.db_path, total_epsilon=total, rng=random.Random(seed))
        self.addCleanup(gw.close)
        gw.register_fictional_v1()
        return gw

    def _items(self) -> list[dict[str, object]]:
        return [
            {
                "item_id": "total",
                "label": "同名显示标签",
                "version": "fictional-v1",
                "operation": "count_all",
                "params": {},
                "epsilon": 1.0,
            },
            {
                "item_id": "beijing",
                "label": "同名显示标签",
                "version": "fictional-v1",
                "operation": "count_by_city",
                "params": {"city": "Beijing"},
                "epsilon": 1.0,
            },
        ]

    def test_preview_is_only_identity_and_budget_plan(self) -> None:
        gw = self._gateway()
        preview = gw.preview_arrangement(self._items(), mode=MODE_PHASED)
        self.assertTrue(preview["approved"] is False)
        self.assertTrue(math.isclose(preview["new_epsilon_required"], 2.0))
        self.assertEqual(len({item["stat_identity"] for item in preview["items"]}), 2)

        serialized = json.dumps(preview, ensure_ascii=False)
        self.assertNotIn("noisy_value", serialized)
        self.assertNotIn("true_count", serialized)
        self.assertNotIn("noise", serialized)
        self.assertTrue(math.isclose(gw.budget()["spent_epsilon"], 0.0))
        self.assertEqual(gw.list_arrangements(), [])
        self.assertEqual(gw.list_releases(), [])

    def test_display_label_does_not_reuse_changed_statistical_identity(self) -> None:
        gw = self._gateway()
        first_items = [
            {
                "item_id": "city",
                "label": "城市人数",
                "version": "fictional-v1",
                "operation": "count_by_city",
                "params": {"city": "Beijing"},
                "epsilon": 1.0,
            }
        ]
        changed_items = [
            {
                "item_id": "city",
                "label": "城市人数",
                "version": "fictional-v1",
                "operation": "count_by_city",
                "params": {"city": "Shanghai"},
                "epsilon": 1.0,
            }
        ]
        gw.approve_arrangement("a", first_items, mode=MODE_ALL)
        gw.fulfill_arrangement("a")
        preview = gw.preview_arrangement(changed_items, mode=MODE_ALL)
        self.assertEqual(preview["items"][0]["budget_action"], "new_release")
        self.assertTrue(math.isclose(preview["new_epsilon_required"], 1.0))

    def test_all_at_once_insufficient_budget_publishes_nothing(self) -> None:
        gw = self._gateway(total=1.5)
        gw.approve_arrangement("group", self._items(), mode=MODE_ALL)
        with self.assertRaises(BudgetExhausted):
            gw.fulfill_arrangement("group")

        arrangement = gw.get_arrangement("group")
        self.assertEqual(arrangement["status"], "approved")
        self.assertTrue(all(item["status"] == "approved" for item in arrangement["items"]))
        self.assertTrue(math.isclose(gw.budget()["spent_epsilon"], 0.0))
        self.assertEqual(gw.list_releases(), [])
        with self.assertRaises(ValueError):
            gw.claim_arrangement_item("group", "total")

    def test_phased_publish_is_permanent_but_unpublished_can_cancel(self) -> None:
        gw = self._gateway(total=1.2)
        gw.approve_arrangement("phased", self._items(), mode=MODE_PHASED)
        first = gw.fulfill_arrangement_item("phased", "total")
        self.assertEqual(first["status"], "partial")
        claimed = gw.claim_arrangement_item("phased", "total")
        self.assertEqual(claimed.item_id, "total")
        self.assertTrue(math.isclose(claimed.epsilon_spent, 0.0))

        with self.assertRaises(BudgetExhausted):
            gw.fulfill_arrangement_item("phased", "beijing")
        canceled = gw.cancel_arrangement("phased")
        self.assertEqual(canceled["status"], "partial")
        statuses = {item["item_id"]: item["status"] for item in canceled["items"]}
        self.assertEqual(statuses, {"total": "claimed", "beijing": "canceled"})
        self.assertTrue(math.isclose(gw.budget()["spent_epsilon"], 1.0))
        self.assertEqual(len(gw.list_releases()), 1)

        same_claim = gw.claim_arrangement_item("phased", "total")
        self.assertEqual(same_claim.value, claimed.value)
        self.assertTrue(math.isclose(gw.budget()["spent_epsilon"], 1.0))

    def test_overlapping_approved_arrangements_share_only_after_fulfillment(self) -> None:
        gw = self._gateway(total=2.0)
        items = self._items()[:1]
        gw.approve_arrangement("a", items, mode=MODE_PHASED)
        gw.approve_arrangement("b", items, mode=MODE_PHASED)

        with self.assertRaises(ValueError):
            gw.claim_arrangement_item("b", "total")
        self.assertTrue(math.isclose(gw.preview_arrangement(items)["new_epsilon_required"], 1.0))

        gw.fulfill_arrangement_item("a", "total")
        preview_b = gw.preview_arrangement(items)
        self.assertEqual(preview_b["items"][0]["budget_action"], "reuse_existing_release")
        self.assertTrue(math.isclose(preview_b["new_epsilon_required"], 0.0))

        gw.fulfill_arrangement_item("b", "total")
        self.assertTrue(math.isclose(gw.budget()["spent_epsilon"], 1.0))
        self.assertEqual(len(gw.list_releases()), 1)
        events = gw.public_audit_log()
        self.assertEqual([event["kind"] for event in events], [
            "arrangement_release",
            "reuse_existing_release",
        ])
        self.assertTrue(math.isclose(sum(event["epsilon"] for event in events), 1.0))

    def test_arrangement_reuses_direct_existing_release(self) -> None:
        gw = self._gateway(total=1.0)
        direct = gw.release(
            "direct-total", "fictional-v1", "count_all", {}, epsilon=1.0
        )
        items = [self._items()[0]]
        preview = gw.preview_arrangement(items, mode=MODE_ALL)
        self.assertEqual(
            preview["items"][0]["existing_release_request_id"], "direct-total"
        )
        self.assertTrue(math.isclose(preview["new_epsilon_required"], 0.0))

        gw.approve_arrangement("reuse-direct", items, mode=MODE_ALL)
        fulfilled = gw.fulfill_arrangement("reuse-direct")
        self.assertEqual(
            fulfilled["items"][0]["source_request_id"], "direct-total"
        )
        claimed = gw.claim_arrangement_item("reuse-direct", "total")
        self.assertEqual(claimed.value, direct.value)
        self.assertTrue(math.isclose(gw.budget()["spent_epsilon"], 1.0))

    def test_legacy_release_without_identity_still_matches_fingerprint(self) -> None:
        gw = self._gateway(total=1.0)
        gw.release("legacy", "fictional-v1", "count_all", {}, epsilon=1.0)
        with open(self.db_path, encoding="utf-8") as handle:
            ledger = json.load(handle)
        del ledger["releases"]["legacy"]["identity"]
        with open(self.db_path, "w", encoding="utf-8") as handle:
            json.dump(ledger, handle, ensure_ascii=False)

        reopened = PrivacyGateway(self.db_path, total_epsilon=1.0, rng=random.Random(8))
        self.addCleanup(reopened.close)
        reopened.register_fictional_v1()
        preview = reopened.preview_arrangement([self._items()[0]], mode=MODE_ALL)
        self.assertEqual(preview["items"][0]["existing_release_request_id"], "legacy")

    def test_duplicate_arrangement_identity_cannot_change_frozen_scope(self) -> None:
        gw = self._gateway()
        gw.approve_arrangement("plan", self._items()[:1], mode=MODE_ALL)
        with self.assertRaises(DuplicateRequest):
            gw.approve_arrangement("plan", self._items()[1:], mode=MODE_ALL)

    def test_states_restart_after_approval_partial_claim_and_cancellation(self) -> None:
        gw = self._gateway(total=1.0)
        gw.approve_arrangement("approved-group", self._items()[:1], mode=MODE_ALL)
        gw.approve_arrangement("phased", self._items(), mode=MODE_PHASED)
        gw.fulfill_arrangement_item("phased", "total")
        original = gw.claim_arrangement_item("phased", "total")
        gw.cancel_arrangement("phased")
        gw.close()

        reopened = PrivacyGateway(self.db_path, total_epsilon=1.0, rng=random.Random(999))
        self.addCleanup(reopened.close)
        reopened.register_fictional_v1()
        self.assertEqual(reopened.get_arrangement("approved-group")["status"], "approved")
        phased = reopened.get_arrangement("phased")
        statuses = {item["item_id"]: item["status"] for item in phased["items"]}
        self.assertEqual(statuses, {"total": "claimed", "beijing": "canceled"})
        claimed = reopened.claim_arrangement_item("phased", "total")
        self.assertEqual(claimed.value, original.value)
        self.assertTrue(math.isclose(reopened.budget()["spent_epsilon"], 1.0))

    def test_public_audit_has_no_exact_statistics_or_noise(self) -> None:
        gw = self._gateway()
        gw.approve_arrangement("group", self._items()[:1], mode=MODE_ALL)
        gw.fulfill_arrangement("group")
        serialized = json.dumps(gw.public_audit_log(), ensure_ascii=False)
        self.assertNotIn("true_count", serialized)
        self.assertNotIn("noise", serialized)
        self.assertNotIn("noisy_value", serialized)

    def test_group_claim_repeats_return_same_results_without_new_charge(self) -> None:
        gw = self._gateway(total=3.0)
        gw.approve_arrangement("group", self._items(), mode=MODE_ALL)
        gw.fulfill_arrangement("group")
        first = gw.claim_arrangement("group")
        self.assertEqual([result.item_id for result in first], ["total", "beijing"])
        self.assertEqual(
            [result.request_id for result in first], ["group:total", "group:beijing"]
        )
        original_values = [result.value for result in first]
        self.assertTrue(all(math.isclose(result.epsilon_spent, 0.0) for result in first))
        gw.close()

        reopened = PrivacyGateway(self.db_path, total_epsilon=3.0, rng=random.Random(4242))
        self.addCleanup(reopened.close)
        reopened.register_fictional_v1()
        repeated = reopened.claim_arrangement("group")
        self.assertEqual([result.value for result in repeated], original_values)
        self.assertTrue(all(result.is_replay for result in repeated))
        self.assertTrue(
            all(math.isclose(result.epsilon_spent, 0.0) for result in repeated)
        )
        self.assertTrue(math.isclose(reopened.budget()["spent_epsilon"], 2.0))
        self.assertEqual(
            sorted(record["request_id"] for record in reopened.list_releases()),
            ["group:beijing", "group:total"],
        )

    def test_phased_cancel_sharing_and_reopen_claims(self) -> None:
        gw = self._gateway(total=1.2)
        gw.approve_arrangement("phased", self._items(), mode=MODE_PHASED)
        first_view = gw.fulfill_arrangement_item("phased", "total")
        self.assertEqual(first_view["status"], "partial")
        first_claim = gw.claim_arrangement_item("phased", "total")

        with self.assertRaises(BudgetExhausted):
            gw.fulfill_arrangement_item("phased", "beijing")
        canceled = gw.cancel_arrangement("phased")
        statuses = {item["item_id"]: item["status"] for item in canceled["items"]}
        self.assertEqual(statuses, {"total": "claimed", "beijing": "canceled"})

        gw.approve_arrangement("shared", self._items()[:1], mode=MODE_ALL)
        shared_view = gw.fulfill_arrangement("shared")
        shared_item = shared_view["items"][0]
        self.assertEqual(shared_item["source_request_id"], "phased:total")
        shared_claim = gw.claim_arrangement_item("shared", "total")
        self.assertEqual(shared_claim.value, first_claim.value)
        self.assertEqual(shared_claim.request_id, "phased:total")
        self.assertTrue(math.isclose(shared_claim.epsilon_spent, 0.0))
        self.assertTrue(math.isclose(gw.budget()["spent_epsilon"], 1.0))

        group_claims = gw.claim_arrangement("phased")
        self.assertEqual([result.item_id for result in group_claims], ["total"])
        self.assertEqual(group_claims[0].value, first_claim.value)
        with self.assertRaises(ValueError):
            gw.claim_arrangement_item("phased", "beijing")
        gw.close()

        reopened = PrivacyGateway(self.db_path, total_epsilon=1.2, rng=random.Random(77))
        self.addCleanup(reopened.close)
        reopened.register_fictional_v1()
        phased = reopened.get_arrangement("phased")
        restarted_statuses = {
            item["item_id"]: item["status"] for item in phased["items"]
        }
        self.assertEqual(
            restarted_statuses, {"total": "claimed", "beijing": "canceled"}
        )
        restarted_claims = reopened.claim_arrangement("phased")
        self.assertEqual(len(restarted_claims), 1)
        self.assertEqual(restarted_claims[0].value, first_claim.value)
        self.assertEqual(restarted_claims[0].request_id, "phased:total")
        self.assertTrue(math.isclose(restarted_claims[0].epsilon_spent, 0.0))
        shared_restarted = reopened.claim_arrangement("shared")
        self.assertEqual(len(shared_restarted), 1)
        self.assertEqual(shared_restarted[0].value, first_claim.value)
        self.assertEqual(shared_restarted[0].request_id, "phased:total")
        self.assertTrue(math.isclose(reopened.budget()["spent_epsilon"], 1.0))
        self.assertEqual(
            [record["request_id"] for record in reopened.list_releases()],
            ["phased:total"],
        )


if __name__ == "__main__":
    unittest.main()
