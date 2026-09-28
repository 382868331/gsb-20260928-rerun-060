"""可发布机制注册表（第一版：有限种计数发布）。

所有统计操作都必须是注册机制，并经 :class:`privacy_stats.publisher.Publisher`
执行。新增统计操作时在此注册，不存在“绕过预算直接读原数据”的路径。

隐私参数约定（计数发布，单位敏感度 1）：

* 相邻数据集定义为相差至多一条记录（add/remove 一条记录）。
* 计数查询的 L1 敏感度恒为 1：增加或删除一个人只让任何谓词计数变化 1。
* 拉普拉斯机制：对真实计数 ```c``` 加 ```Lap(scale = 1/epsilon)``` 噪声，
  满足 (epsilon, 0)-DP。结果四舍五入为整数展示，但内部保留精确带噪值。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from .noise import RandomSource
from .versions import DataVault


@dataclass(frozen=True)
class Mechanism:
    name: str
    epsilon: float
    # (vault, version_id, params, rng) -> (true_value, noisy_value, metadata)
    run: Callable[..., tuple[Any, Any, dict]]
    description: str


def _run_total_count(vault, version_id, params, rng):
    rows = vault.rows_of(version_id)
    true_value = len(rows)
    noisy = true_value + rng.laplace(1.0 / _EPS_TOTAL_COUNT)
    return true_value, noisy, {"kind": "total_count", "sensitivity_l1": 1}


def _run_predicate_count(vault, version_id, params, rng):
    column = params["column"]
    value = params["value"]
    columns = vault.columns_of(version_id)
    if column not in columns:
        raise KeyError(f"列不存在: {column}")
    idx = columns.index(column)
    rows = vault.rows_of(version_id)
    true_value = sum(1 for row in rows if row[idx] == value)
    noisy = true_value + rng.laplace(1.0 / _EPS_PREDICATE_COUNT)
    return (
        true_value,
        noisy,
        {
            "kind": "predicate_count",
            "column": column,
            "value": value,
            "sensitivity_l1": 1,
        },
    )


_EPS_TOTAL_COUNT = 1.0
_EPS_PREDICATE_COUNT = 1.0

REGISTRY: dict[str, Mechanism] = {
    "total_count": Mechanism(
        name="total_count",
        epsilon=_EPS_TOTAL_COUNT,
        run=_run_total_count,
        description="表的总行数计数，epsilon=1.0，Laplace(scale=1)",
    ),
    "predicate_count": Mechanism(
        name="predicate_count",
        epsilon=_EPS_PREDICATE_COUNT,
        run=_run_predicate_count,
        description="某列等于给定值的行数计数，epsilon=1.0，Laplace(scale=1)",
    ),
}


def get_mechanism(name: str) -> Mechanism:
    try:
        return REGISTRY[name]
    except KeyError:
        raise KeyError(
            f"未注册的机制: {name}；可用机制: {sorted(REGISTRY)}"
        ) from None
