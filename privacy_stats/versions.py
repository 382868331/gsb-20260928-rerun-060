"""数据版本库。

每个版本是一份不可变的虚构数据，按内容哈希寻址（content-addressed），
因此同样的数据重复导入只会得到同一个版本号，便于区分“同一版本上的重复请求”
与“新版本上的新查询”。

原始行数据只能通过 :class:`DataVault` 读取；除发布入口在执行已注册机制时
传入的 vault 外，其他代码不应直接访问原始行。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from typing import Iterable, Sequence


@dataclass(frozen=True)
class DataVersion:
    version_id: str
    name: str
    columns: tuple[str, ...]
    rows: tuple[tuple, ...]

    @property
    def row_count(self) -> int:
        return len(self.rows)


class DataVersionStore:
    """基于 SQLite 的不可变数据版本库。"""

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS data_versions (
                version_id TEXT PRIMARY KEY,
                name       TEXT NOT NULL,
                columns_j  TEXT NOT NULL,
                rows_j     TEXT NOT NULL,
                row_count  INTEGER NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """
        )
        self._conn.commit()

    def create_version(
        self,
        name: str,
        columns: Sequence[str],
        rows: Iterable[Sequence],
    ) -> DataVersion:
        """登记一个数据版本；内容相同则返回既有版本号。"""
        cols = tuple(str(c) for c in columns)
        frozen_rows = tuple(tuple(r) for r in rows)
        if any(len(r) != len(cols) for r in frozen_rows):
            raise ValueError("存在列数与表头不一致的行")
        payload = json.dumps(
            [cols, [list(r) for r in frozen_rows]], ensure_ascii=False, sort_keys=True
        )
        version_id = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
        self._conn.execute(
            """
            INSERT INTO data_versions
                (version_id, name, columns_j, rows_j, row_count)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(version_id) DO NOTHING
            """,
            (
                version_id,
                name,
                json.dumps(list(cols), ensure_ascii=False),
                json.dumps([list(r) for r in frozen_rows], ensure_ascii=False),
                len(frozen_rows),
            ),
        )
        self._conn.commit()
        return DataVersion(version_id, name, cols, frozen_rows)

    def get(self, version_id: str) -> DataVersion:
        cur = self._conn.execute(
            "SELECT name, columns_j, rows_j FROM data_versions WHERE version_id = ?",
            (version_id,),
        )
        record = cur.fetchone()
        if record is None:
            raise KeyError(f"未知数据版本: {version_id}")
        name, columns_j, rows_j = record
        columns = tuple(json.loads(columns_j))
        rows = tuple(tuple(r) for r in json.loads(rows_j))
        return DataVersion(version_id, name, columns, rows)


class DataVault:
    """原始数据访问器（唯一合规入口）。

    机制实现通过 vault 读取版本行；统计机制以外的代码不得直接调取行数据，
    从而保证“后续其他统计操作必须经过同一个预算管理入口”。
    """

    def __init__(self, store: DataVersionStore):
        self._store = store

    def rows_of(self, version_id: str) -> tuple[tuple, ...]:
        return self._store.get(version_id).rows

    def columns_of(self, version_id: str) -> tuple[str, ...]:
        return self._store.get(version_id).columns
