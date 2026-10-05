"""发布组合规划：交叠共享、额度竞争、途中追加请求的分阶段核对。"""

from __future__ import annotations

import math
import os
import random
import tempfile
import unittest

from privacy_system import (
    STRATEGY_BALANCED,
    STRATEGY_PRIORITY,
    BudgetExhausted,
    PrivacyGateway,
)
from privacy_system.gateway import ITEM_CANCELED, ITEM_CLAIMED, ITEM_PUBLISHED


def _candidate(
    item_id,
    operation,
    params,
    *,
    epsilon=1.0,
    value=1.0,
    latest_phase=None,
    label=None,
):
    candidate = {
        "item_id": item_id,
        "label": label or item_id,
        "version": "fictional-v1",
        "operation": operation,
        "params": params,
        "epsilon": epsilon,
        "value": value,
    }
    if latest_phase is not None:
        candidate["latest_phase"] = latest_phase
    return candidate


class PortfolioPlanningTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "ledger.json")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _gateway(self, total: float = 4.0, seed: int = 7) -> PrivacyGateway:
        gw = PrivacyGateway(self.db_path, total_epsilon=total, rng=random.Random(seed))
        self.addCleanup(gw.close)
        gw.register_fictional_v1()
        return gw

    def test_plan_does_not_spend_budget_or_create_claimable_results(self) -> None:
        gw = self._gateway()
        plan = gw.plan_portfolio(
            [_candidate("total", "count_all", {}, epsilon=1.0)], phases=2
        )
        self.assertFalse(plan["approved"])
        budget = gw.budget()
        self.assertTrue(math.isclose(budget["spent_epsilon"], 0.0))
        self.assertEqual(gw.list_releases(), [])
        self.assertEqual(gw.list_arrangements(), [])
        with self.assertRaises(KeyError):
            gw.claim_arrangement_item("plan-x", "total")

    def test_overlapping_candidates_share_one_release_and_one_charge(self) -> None:
        gw = self._gateway(total=4.0)
        candidates = [
            _candidate("beijing-a", "count_by_city", {"city": "Beijing"}, epsilon=1.0),
            _candidate("beijing-b", "count_by_city", {"city": "Beijing"}, epsilon=0.5),
            _candidate("shanghai", "count_by_city", {"city": "Shanghai"}, epsilon=1.0),
        ]
        plan = gw.plan_portfolio(candidates, phases=1)
        by_id = {item["item_id"]: item for item in plan["items"]}
        # 同一统计身份只发布一次：取组内最高精度，预算只计一次。
        self.assertEqual(by_id["beijing-a"]["budget_action"], "new_release")
        self.assertEqual(by_id["beijing-b"]["budget_action"], "shared_planned_release")
        self.assertTrue(math.isclose(by_id["beijing-b"]["planned_epsilon"], 1.0))
        self.assertTrue(math.isclose(plan["new_epsilon_required"], 2.0))

        gw.confirm_portfolio("plan-share", candidates, phases=1)
        gw.fulfill_arrangement_phase("plan-share", 1)
        # 三次声明、两次真实发布、两份预算。
        self.assertEqual(len(gw.list_releases()), 2)
        self.assertTrue(math.isclose(gw.budget()["spent_epsilon"], 2.0))
        first = gw.claim_arrangement_item("plan-share", "beijing-a")
        second = gw.claim_arrangement_item("plan-share", "beijing-b")
        self.assertEqual(first.value, second.value)
        self.assertTrue(math.isclose(gw.budget()["spent_epsilon"], 2.0))

    def test_different_params_cannot_share_and_are_charged_separately(self) -> None:
        gw = self._gateway(total=4.0)
        candidates = [
            _candidate("beijing", "count_by_city", {"city": "Beijing"}),
            _candidate("hangzhou", "count_by_city", {"city": "Hangzhou"}),
        ]
        plan = gw.plan_portfolio(candidates, phases=1)
        self.assertTrue(math.isclose(plan["new_epsilon_required"], 2.0))
        gw.confirm_portfolio("plan-params", candidates, phases=1)
        gw.fulfill_arrangement_phase("plan-params", 1)
        self.assertEqual(len(gw.list_releases()), 2)
        self.assertTrue(math.isclose(gw.budget()["spent_epsilon"], 2.0))

    def test_confirm_rechecks_releases_and_remaining_budget(self) -> None:
        gw = self._gateway(total=3.0)
        candidates = [
            _candidate("beijing", "count_by_city", {"city": "Beijing"}, epsilon=1.0, value=3.0),
            _candidate("shanghai", "count_by_city", {"city": "Shanghai"}, epsilon=1.0, value=2.0),
            _candidate("hangzhou", "count_by_city", {"city": "Hangzhou"}, epsilon=1.0, value=1.0),
        ]
        plan = gw.plan_portfolio(candidates, strategy=STRATEGY_PRIORITY, phases=1)
        self.assertTrue(math.isclose(plan["new_epsilon_required"], 3.0))
        # 预演之后、确认之前：另一个请求抢先发布了 Beijing。
        gw.release("req-outside", "fictional-v1", "count_by_city",
                   {"city": "Beijing"}, epsilon=1.5)
        confirmed = gw.confirm_portfolio(
            "plan-recheck", candidates, strategy=STRATEGY_PRIORITY, phases=1
        )
        by_id = {item["item_id"]: item for item in confirmed["items"]}
        # Beijing 变为共享既有发布（ε=0），Shanghai 仍可兑现，Hangzhou 被挤出。
        self.assertEqual(by_id["beijing"]["status"], "approved")
        self.assertEqual(by_id["shanghai"]["status"], "approved")
        self.assertNotIn("hangzhou", by_id)
        self.assertEqual(len(confirmed["rejected"]), 1)
        gap = confirmed["rejected"][0]
        self.assertEqual(gap["item_ids"], ["hangzhou"])
        self.assertEqual(gap["reason"], "budget_exhausted")
        self.assertTrue(math.isclose(gap["epsilon_needed"], 1.0))
        self.assertTrue(math.isclose(gap["epsilon_remaining"], 0.5))
        # 确认本身仍不扣预算；兑现后 Beijing 共享只计一次。
        self.assertTrue(math.isclose(gw.budget()["spent_epsilon"], 1.5))
        gw.fulfill_arrangement_phase("plan-recheck", 1)
        self.assertEqual(len(gw.list_releases()), 2)
        self.assertTrue(math.isclose(gw.budget()["spent_epsilon"], 2.5))

    def test_strategies_differ_on_scarce_budget(self) -> None:
        candidates = [
            _candidate("hot", "count_by_city", {"city": "Beijing"}, epsilon=2.0, value=10.0),
            _candidate("mid", "count_by_city", {"city": "Shanghai"}, epsilon=1.0, value=5.0),
            _candidate("cold", "count_by_city", {"city": "Hangzhou"}, epsilon=1.0, value=1.0),
        ]
        gw = self._gateway(total=3.0)
        priority = gw.plan_portfolio(candidates, strategy=STRATEGY_PRIORITY, phases=1)
        accepted = {item["item_id"] for item in priority["items"]}
        self.assertEqual(accepted, {"hot", "mid"})
        self.assertEqual([r["item_ids"] for r in priority["rejected"]], [["cold"]])

        balanced = gw.plan_portfolio(candidates, strategy=STRATEGY_BALANCED, phases=1)
        by_id = {item["item_id"]: item for item in balanced["items"]}
        self.assertEqual(set(by_id), {"hot", "mid", "cold"})
        self.assertEqual(balanced["rejected"], [])
        # 均衡精度：全部按 3/4 比例压缩 ε。
        self.assertTrue(math.isclose(by_id["hot"]["planned_epsilon"], 1.5))
        self.assertTrue(math.isclose(by_id["mid"]["planned_epsilon"], 0.75))
        self.assertTrue(math.isclose(balanced["new_epsilon_required"], 3.0))

    def test_budget_contention_between_portfolios_keeps_published_parts(self) -> None:
        gw_a = self._gateway(total=3.0, seed=11)
        gw_b = PrivacyGateway(self.db_path, total_epsilon=3.0, rng=random.Random(22))
        self.addCleanup(gw_b.close)
        gw_b.register_fictional_v1()
        gw_a.confirm_portfolio(
            "plan-a",
            [_candidate("a1", "count_by_city", {"city": "Beijing"}, epsilon=2.0)],
            phases=1,
        )
        gw_b.confirm_portfolio(
            "plan-b",
            [_candidate("b1", "count_by_city", {"city": "Shanghai"}, epsilon=2.0)],
            phases=1,
        )
        gw_a.fulfill_arrangement_phase("plan-a", 1)
        # 另一实例基于磁盘最新事实判断：余额不足，整项失败。
        with self.assertRaises(BudgetExhausted):
            gw_b.fulfill_arrangement_phase("plan-b", 1)
        budget = gw_b.budget()
        self.assertTrue(math.isclose(budget["spent_epsilon"], 2.0))
        self.assertTrue(math.isclose(budget["remaining_epsilon"], 1.0))
        # 已发布的部分保留；失败方没有产生任何发布。
        self.assertEqual(len(gw_b.list_releases()), 1)
        item = gw_b.get_arrangement("plan-b")["items"][0]
        self.assertEqual(item["status"], "approved")

    def test_midway_added_requests_see_real_releases_and_remaining(self) -> None:
        gw = self._gateway(total=4.0)
        first = [
            _candidate("beijing", "count_by_city", {"city": "Beijing"},
                       epsilon=1.0, latest_phase=1),
            _candidate("total", "count_all", {}, epsilon=1.0, latest_phase=2),
        ]
        gw.confirm_portfolio("plan-1", first, phases=2)
        gw.fulfill_arrangement_phase("plan-1", 1)
        self.assertTrue(math.isclose(gw.budget()["remaining_epsilon"], 3.0))

        # 途中追加：新计划包含与已发布结果交叠的声明和全新声明。
        followup = [
            _candidate("beijing-again", "count_by_city", {"city": "Beijing"}, epsilon=1.0),
            _candidate("members", "count_by_membership", {"membership": "yes"},
                       epsilon=2.0, value=2.0),
            _candidate("age", "count_by_age_group", {"age_group": "20-29"},
                       epsilon=2.0, value=1.0),
        ]
        plan = gw.plan_portfolio(followup, strategy=STRATEGY_PRIORITY, phases=1)
        by_id = {item["item_id"]: item for item in plan["items"]}
        self.assertEqual(by_id["beijing-again"]["budget_action"], "reuse_existing_release")
        # 剩余 3.0：共享不计费，members(2.0) 接纳，age(2.0) 超出余额被拒绝。
        self.assertTrue(math.isclose(plan["new_epsilon_required"], 2.0))
        self.assertTrue(math.isclose(plan["remaining_epsilon_now"], 3.0))
        self.assertEqual([r["item_ids"] for r in plan["rejected"]], [["age"]])

        confirmed = gw.confirm_portfolio("plan-2", followup, strategy=STRATEGY_PRIORITY, phases=1)
        confirmed_ids = {item["item_id"] for item in confirmed["items"]}
        # 共享不计费后 members(2.0) 与 beijing-again(0) 可接纳，age(2.0) 超出余额。
        self.assertEqual(confirmed_ids, {"beijing-again", "members"})
        self.assertEqual([r["item_ids"] for r in confirmed["rejected"]], [["age"]])

        gw.fulfill_arrangement_phase("plan-2", 1)
        self.assertTrue(math.isclose(gw.budget()["spent_epsilon"], 3.0))
        self.assertEqual(len(gw.list_releases()), 2)
        shared = gw.claim_arrangement_item("plan-2", "beijing-again")
        original = gw.claim_arrangement_item("plan-1", "beijing")
        self.assertEqual(shared.value, original.value)

        # 继续兑现 plan-1 的第二阶段：真实剩余额度仍够。
        gw.fulfill_arrangement_phase("plan-1", 2)
        self.assertTrue(math.isclose(gw.budget()["spent_epsilon"], 4.0))
        self.assertTrue(math.isclose(gw.budget()["remaining_epsilon"], 0.0))

    def test_latest_phase_and_cancel_keep_published_results(self) -> None:
        gw = self._gateway(total=4.0)
        candidates = [
            _candidate("early", "count_by_city", {"city": "Beijing"},
                       epsilon=1.0, latest_phase=1),
            _candidate("late", "count_by_city", {"city": "Shanghai"},
                       epsilon=1.0, latest_phase=2),
        ]
        plan = gw.plan_portfolio(candidates, phases=3)
        by_id = {item["item_id"]: item for item in plan["items"]}
        self.assertEqual(by_id["early"]["phase"], 1)
        self.assertEqual(by_id["late"]["phase"], 2)

        gw.confirm_portfolio("plan-cancel", candidates, phases=3)
        gw.fulfill_arrangement_phase("plan-cancel", 1)
        published = gw.claim_arrangement_item("plan-cancel", "early")
        # 取消未来阶段：不撤销已公开结果，已花预算保留。
        canceled = gw.cancel_arrangement("plan-cancel")
        status = {item["item_id"]: item["status"] for item in canceled["items"]}
        self.assertEqual(status["early"], ITEM_CLAIMED)
        self.assertEqual(status["late"], ITEM_CANCELED)
        self.assertTrue(math.isclose(gw.budget()["spent_epsilon"], 1.0))
        again = gw.claim_arrangement_item("plan-cancel", "early")
        self.assertEqual(again.value, published.value)
        with self.assertRaises(ValueError):
            gw.claim_arrangement_item("plan-cancel", "late")
        # 已取消的阶段不再兑现：无新发布、预算不变。
        gw.fulfill_arrangement_phase("plan-cancel", 2)
        self.assertEqual(len(gw.list_releases()), 1)
        self.assertTrue(math.isclose(gw.budget()["spent_epsilon"], 1.0))

    def test_unfulfilled_plan_items_are_not_claimable(self) -> None:
        gw = self._gateway(total=4.0)
        gw.confirm_portfolio(
            "plan-pending",
            [_candidate("beijing", "count_by_city", {"city": "Beijing"}, epsilon=1.0)],
            phases=1,
        )
        self.assertTrue(math.isclose(gw.budget()["spent_epsilon"], 0.0))
        self.assertEqual(gw.list_releases(), [])
        with self.assertRaises(ValueError):
            gw.claim_arrangement_item("plan-pending", "beijing")


if __name__ == "__main__":
    unittest.main()
