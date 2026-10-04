"""发布安排（预演/批准/兑现/领取）的确定性测试。

覆盖边界：
- 预演只暴露身份/共享事实/预算去向，不产生可领取结果、不泄露数值；
- 统计身份由内容决定：改筛选范围或同名不同内容都产生新身份；
- atomic 整组兑现：预算不足则一项都不发布、不扣预算；
- staged 分阶段兑现：已发布部分永久保留，未发布部分可取消；
- 交叠安排共享同一统计：只由首次兑现消耗预算，不重复消耗、不提前泄露；
- 批准后取消/部分领取等状态落盘，重启后结果与账目一致；
- 新数据版本不影响旧安排，重新研究产生独立安排；
- 公开审计只含发布事实与预算变化。
"""

from __future__ import annotations

import os
import random
import tempfile
import unittest

from privacy_system import (
    BudgetExhausted,
    InvalidPlanState,
    PrivacyGateway,
    UnknownPlan,
)
from privacy_system.fictional_data import fictional_dataset


def _items(*specs):
    return [
        {"operation": op, "params": params, "epsilon": eps}
        for op, params, eps in specs
    ]


class PlanTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "ledger.json")
        self.seed = 20261004

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _gateway(self, total: float = 10.0) -> PrivacyGateway:
        gw = PrivacyGateway(self.db_path, total_epsilon=total, rng=random.Random(self.seed))
        self.addCleanup(gw.close)
        gw.register_fictional_v1()
        return gw


class PreviewTest(PlanTestBase):
    def test_preview_shows_identity_and_budget_only(self) -> None:
        gw = self._gateway()
        items = _items(
            ("count_all", {}, 1.0),
            ("count_by_city", {"city": "Beijing"}, 2.0),
        )
        preview = gw.preview_plan("fictional-v1", items, name="城市组")
        self.assertEqual(preview["total_fresh_epsilon"], 3.0)
        self.assertTrue(preview["budget_sufficient"])
        self.assertEqual(preview["remaining_epsilon"], 10.0)
        # 预演不产生可领取结果：没有任何数值字段
        forbidden = {"value", "noisy_value", "true_count", "noise"}
        def walk(node):
            if isinstance(node, dict):
                for key, val in node.items():
                    self.assertNotIn(key, forbidden)
                    walk(val)
            elif isinstance(node, list):
                for val in node:
                    walk(val)
        walk(preview)
        # 预演不改变账本：无安排、无发布、预算未动
        self.assertEqual(gw.list_plans(), [])
        self.assertEqual(gw.list_releases(), [])
        self.assertEqual(gw.budget()["spent_epsilon"], 0.0)

    def test_preview_marks_shareable_existing_release(self) -> None:
        gw = self._gateway()
        gw.release("adhoc-1", "fictional-v1", "count_all", {}, epsilon=1.0)
        items = _items(
            ("count_all", {}, 1.0),                       # 与既有发布同身份
            ("count_by_city", {"city": "Beijing"}, 1.0),  # 新统计
        )
        preview = gw.preview_plan("fictional-v1", items)
        self.assertEqual(preview["items"][0]["shared_from"], "adhoc-1")
        self.assertEqual(preview["items"][0]["fresh_epsilon"], 0.0)
        self.assertIsNone(preview["items"][1]["shared_from"])
        self.assertEqual(preview["total_fresh_epsilon"], 1.0)

    def test_identity_changes_with_scope_not_name(self) -> None:
        gw = self._gateway()
        a = gw.preview_plan("fictional-v1", _items(("count_by_city", {"city": "Beijing"}, 1.0)), name="同名")
        b = gw.preview_plan("fictional-v1", _items(("count_by_city", {"city": "Shanghai"}, 1.0)), name="同名")
        c = gw.preview_plan("fictional-v1", _items(("count_by_city", {"city": "Beijing"}, 1.0)), name="另一个名字")
        d = gw.preview_plan("fictional-v1", _items(("count_by_city", {"city": "Beijing"}, 1.0)), mode="staged", name="同名")
        self.assertNotEqual(a["plan_id"], b["plan_id"])  # 筛选范围不同 → 新身份
        self.assertEqual(a["plan_id"], c["plan_id"])     # 显示名不影响身份
        self.assertNotEqual(a["plan_id"], d["plan_id"])  # 兑现模式不同 → 新身份


class AtomicPlanTest(PlanTestBase):
    def test_atomic_fulfill_all_or_nothing(self) -> None:
        gw = self._gateway(total=2.5)
        items = _items(
            ("count_all", {}, 1.0),
            ("count_by_city", {"city": "Beijing"}, 1.0),
            ("count_by_membership", {"membership": "yes"}, 1.0),
        )
        plan_id = gw.approve_plan("fictional-v1", items, name="整组")
        with self.assertRaises(BudgetExhausted):
            gw.fulfill_plan(plan_id)
        # 一项都不对外可领取，预算分文未动
        self.assertEqual(gw.claim_plan(plan_id), [])
        self.assertEqual(gw.list_releases(), [])
        self.assertEqual(gw.budget()["spent_epsilon"], 0.0)
        self.assertEqual(gw.plan_status(plan_id)["status"], "approved")

    def test_atomic_fulfill_success_and_replay(self) -> None:
        gw = self._gateway(total=5.0)
        items = _items(
            ("count_all", {}, 1.0),
            ("count_by_city", {"city": "Beijing"}, 1.0),
        )
        plan_id = gw.approve_plan("fictional-v1", items)
        status = gw.fulfill_plan(plan_id)
        self.assertEqual(status["status"], "fulfilled")
        self.assertEqual(gw.budget()["spent_epsilon"], 2.0)
        claimed = gw.claim_plan(plan_id)
        self.assertEqual(len(claimed), 2)
        values = {r.request_id: r.value for r in claimed}
        # 重复兑现幂等：同一结果、不二次扣账
        again = gw.fulfill_plan(plan_id)
        self.assertEqual(gw.budget()["spent_epsilon"], 2.0)
        for entry in again["items"]:
            self.assertEqual(entry["noisy_value"], values[entry["request_id"]])


class StagedPlanTest(PlanTestBase):
    def test_staged_fulfill_then_cancel_keeps_published(self) -> None:
        gw = self._gateway(total=5.0)
        items = _items(
            ("count_all", {}, 1.0),
            ("count_by_city", {"city": "Beijing"}, 1.0),
            ("count_by_membership", {"membership": "yes"}, 1.0),
        )
        plan_id = gw.approve_plan("fictional-v1", items, mode="staged")
        gw.fulfill_plan(plan_id, [0])
        status = gw.plan_status(plan_id)
        self.assertEqual(status["status"], "partially_fulfilled")
        self.assertEqual(gw.budget()["spent_epsilon"], 1.0)
        # 取消未发布部分
        gw.cancel_plan(plan_id)
        status = gw.plan_status(plan_id)
        self.assertEqual(status["status"], "cancelled")
        self.assertEqual(status["items"][0]["status"], "fulfilled")
        self.assertEqual(status["items"][1]["status"], "cancelled")
        # 已发布部分永久保留：仍可领取，预算消耗保留
        self.assertEqual(len(gw.claim_plan(plan_id)), 1)
        self.assertEqual(gw.budget()["spent_epsilon"], 1.0)
        # 已取消项不能再兑现
        with self.assertRaises(InvalidPlanState):
            gw.fulfill_plan(plan_id, [1])

    def test_atomic_plan_rejects_partial_fulfill(self) -> None:
        gw = self._gateway()
        plan_id = gw.approve_plan("fictional-v1", _items(("count_all", {}, 1.0)))
        with self.assertRaises(ValueError):
            gw.fulfill_plan(plan_id, [0])


class OverlapPlanTest(PlanTestBase):
    def test_shared_statistic_charged_once(self) -> None:
        gw = self._gateway(total=3.0)
        shared = ("count_by_city", {"city": "Beijing"}, 1.0)
        plan_a = gw.approve_plan("fictional-v1", _items(shared, ("count_all", {}, 1.0)), mode="staged", name="甲")
        plan_b = gw.approve_plan("fictional-v1", _items(shared, ("count_by_membership", {"membership": "yes"}, 1.0)), mode="staged", name="乙")
        # 甲先兑现共享项：由甲的这次兑现消耗预算
        gw.fulfill_plan(plan_a, [0])
        self.assertEqual(gw.budget()["spent_epsilon"], 1.0)
        rid_a = gw.plan_status(plan_a)["items"][0]["request_id"]
        # 乙兑现同一统计：引用既有事实，不重复消耗
        gw.fulfill_plan(plan_b, [0])
        item_b = gw.plan_status(plan_b)["items"][0]
        self.assertEqual(item_b["shared_from"], rid_a)
        self.assertEqual(item_b["epsilon_charged"], 0.0)
        self.assertEqual(gw.budget()["spent_epsilon"], 1.0)
        # 两个安排领取到同一份公开结果
        va = gw.claim_plan(plan_a)[0].value
        vb = gw.claim_plan(plan_b)[0].value
        self.assertEqual(va, vb)

    def test_no_leak_before_other_plan_publishes(self) -> None:
        gw = self._gateway(total=5.0)
        shared = ("count_by_city", {"city": "Beijing"}, 1.0)
        plan_a = gw.approve_plan("fictional-v1", _items(shared), mode="staged", name="甲")
        plan_b = gw.approve_plan("fictional-v1", _items(shared), mode="staged", name="乙")
        # 甲尚未兑现：乙的预演不能把甲的待发布统计当作可共享事实
        preview_b = gw.preview_plan("fictional-v1", _items(shared))
        self.assertIsNone(preview_b["items"][0]["shared_from"])
        self.assertEqual(preview_b["total_fresh_epsilon"], 1.0)
        # 乙也不能领取到任何结果
        self.assertEqual(gw.claim_plan(plan_b), [])
        status_b = gw.plan_status(plan_b)
        self.assertNotIn("noisy_value", status_b["items"][0])
        # 甲兑现后，乙的预演才显示可共享
        gw.fulfill_plan(plan_a, [0])
        preview_b2 = gw.preview_plan("fictional-v1", _items(shared))
        self.assertIsNotNone(preview_b2["items"][0]["shared_from"])

    def test_competing_plans_contend_for_balance(self) -> None:
        gw = self._gateway(total=1.5)
        plan_a = gw.approve_plan("fictional-v1", _items(("count_all", {}, 1.0)), name="甲")
        plan_b = gw.approve_plan("fictional-v1", _items(("count_by_city", {"city": "Beijing"}, 1.0)), name="乙")
        gw.fulfill_plan(plan_a)
        with self.assertRaises(BudgetExhausted):
            gw.fulfill_plan(plan_b)
        # 失败的安排保持已批准未兑现，余额如实
        self.assertEqual(gw.plan_status(plan_b)["status"], "approved")
        self.assertAlmostEqual(gw.budget()["remaining_epsilon"], 0.5)


class RestartPersistenceTest(PlanTestBase):
    def test_restart_preserves_results_and_ledger(self) -> None:
        gw = self._gateway(total=5.0)
        version = gw.register_fictional_v1()
        plan_a = gw.approve_plan(version, _items(
            ("count_all", {}, 1.0),
            ("count_by_city", {"city": "Beijing"}, 1.0),
        ), mode="staged", name="分阶段")
        plan_b = gw.approve_plan(version, _items(("count_all", {}, 1.0)), mode="staged", name="共享")
        gw.fulfill_plan(plan_a, [0])          # 已兑现
        claimed_before = gw.claim_plan(plan_a)
        plan_c = gw.approve_plan(version, _items(("count_by_membership", {"membership": "yes"}, 1.0)), name="将取消")
        gw.cancel_plan(plan_c)                # 刚取消
        spent_before = gw.budget()["spent_epsilon"]
        gw.close()

        # 重开：同一账本、同一数据，继续得到同一结果与完整账目
        gw2 = PrivacyGateway(self.db_path, total_epsilon=5.0, rng=random.Random(self.seed))
        self.addCleanup(gw2.close)
        gw2.register_fictional_v1()
        self.assertEqual(gw2.budget()["spent_epsilon"], spent_before)
        claimed_after = gw2.claim_plan(plan_a)
        self.assertEqual(
            [r.value for r in claimed_after], [r.value for r in claimed_before]
        )
        self.assertEqual(gw2.plan_status(plan_c)["status"], "cancelled")
        # 未兑现项可继续兑现；共享项引用既有事实不再扣预算
        gw2.fulfill_plan(plan_b, [0])
        self.assertEqual(gw2.budget()["spent_epsilon"], spent_before)
        self.assertEqual(
            gw2.plan_status(plan_b)["items"][0]["shared_from"],
            gw2.plan_status(plan_a)["items"][0]["request_id"],
        )
        gw2.fulfill_plan(plan_a, [1])
        self.assertAlmostEqual(gw2.budget()["spent_epsilon"], spent_before + 1.0)


class NewDataTest(PlanTestBase):
    def test_plan_bound_to_approval_version(self) -> None:
        gw = self._gateway(total=10.0)
        items = _items(("count_all", {}, 1.0))
        plan_v1 = gw.approve_plan("fictional-v1", items, name="总行数")
        # 新资料到达 → 新版本；同名同操作的重新研究是独立安排
        columns, rows = fictional_dataset()
        new_rows = tuple(rows) + (dict(rows[0]),)
        gw.register_version("fictional-v2", (columns, new_rows), description="第二批")
        plan_v2 = gw.approve_plan("fictional-v2", items, name="总行数")
        self.assertNotEqual(plan_v1, plan_v2)
        # 旧安排仍绑定批准时的数据范围
        gw.fulfill_plan(plan_v1)
        self.assertEqual(gw.plan_status(plan_v1)["version"], "fictional-v1")
        audit = gw.audit_release(gw.plan_status(plan_v1)["items"][0]["request_id"])
        self.assertEqual(audit["true_count"], 10)
        gw.fulfill_plan(plan_v2)
        audit2 = gw.audit_release(gw.plan_status(plan_v2)["items"][0]["request_id"])
        self.assertEqual(audit2["true_count"], 11)


class PublicAuditTest(PlanTestBase):
    def test_audit_events_only_facts_and_budget(self) -> None:
        gw = self._gateway(total=5.0)
        plan_id = gw.approve_plan("fictional-v1", _items(
            ("count_all", {}, 1.0),
            ("count_by_city", {"city": "Beijing"}, 2.0),
        ), mode="staged")
        gw.fulfill_plan(plan_id, [0])
        gw.cancel_plan(plan_id)
        events = gw.audit_plan_events()
        kinds = [e["event"] for e in events]
        self.assertEqual(kinds, ["approved", "fulfilled_item", "cancelled"])
        charged = [e["epsilon_charged"] for e in events]
        self.assertEqual(charged, [0.0, 1.0, 0.0])
        forbidden = {"value", "noisy_value", "true_count", "noise"}
        for event in events:
            self.assertTrue(forbidden.isdisjoint(event.keys()))

    def test_unknown_plan_raises(self) -> None:
        gw = self._gateway()
        with self.assertRaises(UnknownPlan):
            gw.plan_status("plan-不存在")
        with self.assertRaises(UnknownPlan):
            gw.fulfill_plan("plan-不存在")


if __name__ == "__main__":
    unittest.main()

