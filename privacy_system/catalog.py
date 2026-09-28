"""第一版允许发布的“有限种计数查询”目录。

只有在本模块注册的操作才能被网关执行；网关之外不存在直接读取
原始数据的公开路径，因此后续新增统计操作时，必须在目录中登记
敏感度并经过同一个预算入口。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from .errors import UnknownOperation


@dataclass(frozen=True)
class CountOperation:
    name: str
    description: str
    sensitivity: float
    allowed_filters: frozenset[str]
    compute: Callable[[tuple[tuple[str, ...], tuple[dict, ...]], dict[str, str]], int]

    def validate_params(self, params: dict[str, str]) -> None:
        unknown = set(params) - self.allowed_filters
        if unknown:
            raise ValueError(f"操作 {self.name!r} 不支持过滤条件: {sorted(unknown)}")


def _count_all(data, params: dict[str, str]) -> int:
    if params:
        raise ValueError("count_all 不接受过滤参数")
    _columns, rows = data
    return len(rows)


def _count_by_city(data, params: dict[str, str]) -> int:
    _columns, rows = data
    return sum(1 for row in rows if row["city"] == params["city"])


def _count_by_age_group(data, params: dict[str, str]) -> int:
    _columns, rows = data
    return sum(1 for row in rows if row["age_group"] == params["age_group"])


def _count_by_membership(data, params: dict[str, str]) -> int:
    _columns, rows = data
    return sum(1 for row in rows if row["membership"] == params["membership"])


OPERATIONS: dict[str, CountOperation] = {
    op.name: op
    for op in (
        CountOperation(
            name="count_all",
            description="虚构数据总行数",
            sensitivity=1.0,
            allowed_filters=frozenset(),
            compute=_count_all,
        ),
        CountOperation(
            name="count_by_city",
            description="按城市计数",
            sensitivity=1.0,
            allowed_filters=frozenset({"city"}),
            compute=_count_by_city,
        ),
        CountOperation(
            name="count_by_age_group",
            description="按年龄段计数",
            sensitivity=1.0,
            allowed_filters=frozenset({"age_group"}),
            compute=_count_by_age_group,
        ),
        CountOperation(
            name="count_by_membership",
            description="按是否会员计数",
            sensitivity=1.0,
            allowed_filters=frozenset({"membership"}),
            compute=_count_by_membership,
        ),
    )
}


def get_operation(name: str) -> CountOperation:
    try:
        return OPERATIONS[name]
    except KeyError as exc:
        raise UnknownOperation(f"未注册的操作: {name!r}") from exc
