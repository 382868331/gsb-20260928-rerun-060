"""SQLite 连接助手。

使用 ```isolation_level=None```（驱动层 autocommit），事务完全由
:class:`privacy_stats.publisher.Publisher` 显式管理，避免驱动隐式事务与
```BEGIN IMMEDIATE``` 冲突。
"""

from __future__ import annotations

import sqlite3


def open_connection(path: str = ":memory:") -> sqlite3.Connection:
    conn = sqlite3.connect(path, isolation_level=None)
    conn.row_factory = None
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    return conn
