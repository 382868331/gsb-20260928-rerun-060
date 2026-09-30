"""JSON 文件预算账本：发布记录与预算余额原子落盘。

每次提交都把“余额 + 发布记录”作为一个整体写入临时文件，
``flush`` + ``fsync`` 后用 :func:`os.replace` 原子替换，
因此发布结果和扣预算要么同时生效，要么都不生效；
进程崩溃或重启后不会出现“已发布但未扣账”的重复发放。
"""

from __future__ import annotations

import json
import os
import tempfile
from typing import Any


class FileLedger:
    def __init__(self, path: str, total_epsilon: float, composition: str) -> None:
        self.path = path
        self.in_memory = path == ":memory:"
        if self.in_memory:
            self._state: dict[str, Any] = {
                "total_epsilon": total_epsilon,
                "spent_epsilon": 0.0,
                "composition": composition,
                "versions": {},
                "releases": {},
            }
            return
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as handle:
                self._state = json.load(handle)
        else:
            directory = os.path.dirname(os.path.abspath(path))
            os.makedirs(directory, exist_ok=True)
            self._state = {
                "total_epsilon": total_epsilon,
                "spent_epsilon": 0.0,
                "composition": composition,
                "versions": {},
                "releases": {},
            }
            self._flush()

    @property
    def state(self) -> dict[str, Any]:
        return self._state

    def commit(self, mutate) -> Any:
        """在一份深拷贝上执行变更；成功后整体原子落盘。

        ``mutate`` 抛异常则放弃全部修改（余额与发布记录都不变）。
        先完成落盘再替换内存态：若落盘失败（如磁盘错误、进程被杀），
        内存与磁盘都保持提交前状态，不会出现“内存已扣账但磁盘丢失”。
        """
        import copy

        candidate = copy.deepcopy(self._state)
        result = mutate(candidate)
        if not self.in_memory:
            self._flush(candidate)
        self._state = candidate
        return result

    def _flush(self, state: dict[str, Any] | None = None) -> None:
        state = self._state if state is None else state
        directory = os.path.dirname(os.path.abspath(self.path))
        fd, tmp_name = tempfile.mkstemp(prefix=".ledger-", suffix=".tmp", dir=directory)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(state, handle, ensure_ascii=False, indent=2, sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_name, self.path)
            dir_fd = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except BaseException:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
            raise
