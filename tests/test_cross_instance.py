"""跨实例（线程/进程）与提交中途退出场景的一致性测试。

覆盖题目要求：

* 两个发布实例共用一份账本时，相同请求对应同一发布事实、只扣一次预算；
* 不同请求争用最后一点预算，不能同时越额成功；
* 提交中途退出后可查询与接续，区分“尚未发布 / 已发布未收到 / 预算不足”；
* 重开（新进程读回同一账本）后公开输出与账本一致。
"""

from __future__ import annotations

import math
import multiprocessing as mp
import os
import random
import tempfile
import threading
import unittest

from privacy_system import (
    BudgetExhausted,
    PrivacyGateway,
    ReleaseInterrupted,
)


def _worker_release(db_path, request_id, epsilon, queue):
    gw = PrivacyGateway(
        db_path,
        total_epsilon=3.0,
        rng=random.Random(hash(request_id) % 10_000),
    )
    try:
        gw.register_fictional_v1()
        result = gw.release(request_id, "fictional-v1", "count_all", {}, epsilon=epsilon)
        queue.put(("ok", result.value, result.is_replay))
    except BudgetExhausted:
        queue.put(("rejected", None, None))
    finally:
        gw.close()


def _worker_drop_then_exit(db_path, request_id, epsilon):
    gw = PrivacyGateway(db_path, total_epsilon=5.0, rng=random.Random(5))
    gw.register_fictional_v1()
    gw.fail_next_delivery(request_id)
    try:
        gw.release(request_id, "fictional-v1", "count_all", {}, epsilon=epsilon)
    except ReleaseInterrupted:
        pass
    gw.close()


class CrossInstanceTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "ledger.json")

    def tearDown(self):
        self.tmp.cleanup()

    def _two_instances(self, total=3.0):
        gw1 = PrivacyGateway(self.db_path, total_epsilon=total, rng=random.Random(1))
        gw2 = PrivacyGateway(self.db_path, total_epsilon=total, rng=random.Random(2))
        self.addCleanup(gw1.close)
        self.addCleanup(gw2.close)
        gw1.register_fictional_v1()
        gw2.register_fictional_v1()
        return gw1, gw2

    def test_shared_ledger_same_request_is_single_fact(self):
        gw1, gw2 = self._two_instances()
        first = gw1.release("shared", "fictional-v1", "count_all", {}, epsilon=1.0)
        second = gw2.release("shared", "fictional-v1", "count_all", {}, epsilon=1.0)
        self.assertEqual(first.value, second.value)
        self.assertTrue(second.is_replay)
        self.assertFalse(first.is_replay)
        self.assertTrue(math.isclose(gw1.budget()["spent_epsilon"], 1.0))
        self.assertTrue(math.isclose(gw2.budget()["spent_epsilon"], 1.0))
        self.assertEqual(len(gw1.list_releases()), 1)
        self.assertEqual(len(gw2.list_releases()), 1)
        self.assertEqual(
            gw1.list_releases()[0]["noisy_value"],
            gw2.list_releases()[0]["noisy_value"],
        )

    def test_separate_processes_same_request_charge_once(self):
        ctx = mp.get_context("fork")
        queue = ctx.Queue()
        procs = [
            ctx.Process(
                target=_worker_release, args=(self.db_path, "p-shared", 1.0, queue)
            )
            for _ in range(2)
        ]
        for p in procs:
            p.start()
        outcomes = [queue.get() for _ in procs]
        for p in procs:
            p.join()
        values = {o[1] for o in outcomes}
        replayed = sum(1 for o in outcomes if o[2])
        self.assertEqual(len(values), 1)
        self.assertEqual(replayed, 1)
        gw = PrivacyGateway(self.db_path, total_epsilon=3.0, rng=random.Random(3))
        self.addCleanup(gw.close)
        gw.register_fictional_v1()
        self.assertTrue(math.isclose(gw.budget()["spent_epsilon"], 1.0))
        self.assertEqual(len(gw.list_releases()), 1)

    def test_two_instances_contend_last_budget(self):
        # 每次 2.0、总额 3.0：两个实例并发争用，只有一个成功，另一个拒绝。
        gw1, gw2 = self._two_instances(total=3.0)
        outcomes = []
        barrier = threading.Barrier(2)

        def contend(gw, rid):
            barrier.wait()
            try:
                gw.release(rid, "fictional-v1", "count_all", {}, epsilon=2.0)
                outcomes.append("ok")
            except BudgetExhausted:
                outcomes.append("rejected")

        threads = [
            threading.Thread(target=contend, args=(gw1, "w-1")),
            threading.Thread(target=contend, args=(gw2, "w-2")),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(sorted(outcomes), ["ok", "rejected"])
        self.assertTrue(math.isclose(gw1.budget()["spent_epsilon"], 2.0))
        self.assertTrue(math.isclose(gw2.budget()["spent_epsilon"], 2.0))
        self.assertEqual(len(gw1.list_releases()), 1)

    def test_many_processes_contend_budget_no_overspend(self):
        ctx = mp.get_context("fork")
        queue = ctx.Queue()
        n = 6
        procs = [
            ctx.Process(
                target=_worker_release, args=(self.db_path, f"m-{i}", 1.0, queue)
            )
            for i in range(n)
        ]
        for p in procs:
            p.start()
        outcomes = [queue.get() for _ in procs]
        for p in procs:
            p.join()
        self.assertEqual(sum(1 for o in outcomes if o[0] == "ok"), 3)
        self.assertEqual(sum(1 for o in outcomes if o[0] == "rejected"), n - 3)
        gw = PrivacyGateway(self.db_path, total_epsilon=3.0, rng=random.Random(4))
        self.addCleanup(gw.close)
        gw.register_fictional_v1()
        self.assertTrue(math.isclose(gw.budget()["spent_epsilon"], 3.0))
        self.assertEqual(len(gw.list_releases()), 3)

    def test_interrupted_commit_query_and_resume(self):
        gw = PrivacyGateway(self.db_path, total_epsilon=5.0, rng=random.Random(7))
        self.addCleanup(gw.close)
        gw.register_fictional_v1()

        self.assertEqual(gw.request_status("z")["status"], "not_released")
        self.assertIsNone(gw.get_release("z"))

        gw.fail_next_delivery("z")
        with self.assertRaises(ReleaseInterrupted):
            gw.release("z", "fictional-v1", "count_all", {}, epsilon=2.0)

        self.assertTrue(math.isclose(gw.budget()["spent_epsilon"], 2.0))
        self.assertEqual(
            gw.request_status("z")["status"], "published_unacknowledged"
        )

        resumed = gw.get_release("z")
        assert resumed is not None
        self.assertTrue(resumed.is_replay)
        self.assertTrue(math.isclose(gw.budget()["spent_epsilon"], 2.0))
        self.assertEqual(gw.request_status("z")["status"], "published")

        replay = gw.release("z", "fictional-v1", "count_all", {}, epsilon=2.0)
        self.assertTrue(replay.is_replay)
        self.assertEqual(replay.value, resumed.value)
        self.assertTrue(math.isclose(gw.budget()["spent_epsilon"], 2.0))

    def test_interrupted_then_new_process_resumes(self):
        ctx = mp.get_context("fork")
        p = ctx.Process(target=_worker_drop_then_exit, args=(self.db_path, "crash", 2.0))
        p.start()
        p.join()
        self.assertEqual(p.exitcode, 0)

        gw = PrivacyGateway(self.db_path, total_epsilon=5.0, rng=random.Random(99))
        self.addCleanup(gw.close)
        gw.register_fictional_v1()
        self.assertTrue(math.isclose(gw.budget()["spent_epsilon"], 2.0))
        self.assertEqual(
            gw.request_status("crash")["status"], "published_unacknowledged"
        )
        resumed = gw.get_release("crash")
        assert resumed is not None
        self.assertTrue(resumed.is_replay)
        self.assertEqual(gw.request_status("crash")["status"], "published")
        self.assertTrue(math.isclose(gw.budget()["spent_epsilon"], 2.0))

    def test_budget_exhausted_distinct_from_unreleased(self):
        gw = PrivacyGateway(self.db_path, total_epsilon=1.0, rng=random.Random(8))
        self.addCleanup(gw.close)
        gw.register_fictional_v1()
        with self.assertRaises(BudgetExhausted):
            gw.release("no-budget", "fictional-v1", "count_all", {}, epsilon=2.0)
        self.assertEqual(gw.request_status("no-budget")["status"], "not_released")
        self.assertIsNone(gw.get_release("no-budget"))
        self.assertTrue(math.isclose(gw.budget()["spent_epsilon"], 0.0))


if __name__ == "__main__":
    unittest.main()

