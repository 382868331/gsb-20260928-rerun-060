"""第一版内置的虚构示例数据（非真实个人信息）。"""

from __future__ import annotations

FICTIONAL_COLUMNS = ("id", "city", "age_group", "membership")

FICTIONAL_ROWS = (
    {"id": 1, "city": "Beijing", "age_group": "20-29", "membership": "yes"},
    {"id": 2, "city": "Shanghai", "age_group": "30-39", "membership": "no"},
    {"id": 3, "city": "Beijing", "age_group": "20-29", "membership": "no"},
    {"id": 4, "city": "Shenzhen", "age_group": "40-49", "membership": "yes"},
    {"id": 5, "city": "Beijing", "age_group": "30-39", "membership": "yes"},
    {"id": 6, "city": "Shanghai", "age_group": "20-29", "membership": "yes"},
    {"id": 7, "city": "Hangzhou", "age_group": "40-49", "membership": "no"},
    {"id": 8, "city": "Beijing", "age_group": "50+", "membership": "no"},
    {"id": 9, "city": "Shenzhen", "age_group": "30-39", "membership": "yes"},
    {"id": 10, "city": "Shanghai", "age_group": "50+", "membership": "yes"},
)


def fictional_dataset() -> tuple[tuple[str, ...], tuple[dict, ...]]:
    """返回 (列名, 行) 的只读副本。"""
    return FICTIONAL_COLUMNS, tuple(dict(row) for row in FICTIONAL_ROWS)
