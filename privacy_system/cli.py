"""命令行演示：展示有限计数发布、预算账与幂等重放。"""

from __future__ import annotations

import argparse
import random

from .errors import BudgetExhausted
from .gateway import PrivacyGateway


def main() -> None:
    parser = argparse.ArgumentParser(description="隐私统计发布系统演示")
    parser.add_argument("--db", default="privacy_ledger.json", help="账本文件路径")
    parser.add_argument("--total-epsilon", type=float, default=10.0)
    parser.add_argument("--seed", type=int, default=20260929, help="加噪随机源种子")
    args = parser.parse_args()

    with PrivacyGateway(
        args.db, total_epsilon=args.total_epsilon, rng=random.Random(args.seed)
    ) as gw:
        version = gw.register_fictional_v1()
        print("数据版本:", gw.list_versions())
        print("初始预算:", gw.budget())

        plan = [
            ("q1", "count_all", {}),
            ("q2", "count_by_city", {"city": "Beijing"}),
            ("q3", "count_by_age_group", {"age_group": "20-29"}),
            ("q1", "count_all", {}),  # 重复请求：应重放且不扣预算
            ("q4", "count_by_membership", {"membership": "yes"}),
            ("q5", "count_by_city", {"city": "Shanghai"}),
            ("q6", "count_all", {}),  # 新查询，即使与 q1 相同也要扣预算
        ]
        for request_id, operation, params in plan:
            try:
                result = gw.release(
                    request_id, version, operation, params, epsilon=2.0
                )
            except BudgetExhausted as exc:
                print(f"[{request_id}] 拒绝：{exc}")
                continue
            kind = "重放(不扣预算)" if result.is_replay else "新发布"
            print(
                f"[{request_id}] {operation}({params}) {kind}: "
                f"带噪结果={result.value:.4f}（非真实精确值）, "
                f"剩余ε={result.epsilon_remaining:g}"
            )
        print("最终预算:", gw.budget())


if __name__ == "__main__":
    main()
