"""JSON 文件预算账本：发布记录与预算余额原子落盘。

每次事务都把“余额 + 发布记录”作为一个整体写入临时文件，
``flush`` + ``fsync`` 后用 :func:`os.replace` 原子替换，
因此发布结果和扣预算要么同时生效，要么都不生效；
进程崩溃或重启后不会出现“已发布但未扣账”的重复发放。

多个 :class:`~privacy_system.gateway.PrivacyGateway` 实例（包括不同进程）
可以共用同一份账本文件：每个事务都持有一把与账本文件绑定的
``flock`` 排他锁，并在锁内重新读取磁盘上的最新状态，
因此跨实例的查重、预算校验与落盘整体串行，不会出现丢失更新。
"""

from __future__ import annotations

import copy
import fcntl
import json
import os
import tempfile
import threading
from typing import Any


class FileLedger:
    def __init__(
        self,
        path: str,
        total_epsilon: float,
        composition: str,
    ) -> None:
        self.path = path
        self.in_memory = path == ":memory:"
        self._mem_lock = threading.RLock()
        self._lock_fd = None
        if self.in_memory:
            self._state = self._initial_state(total_epsilon, composition)
            return
        directory = os.path.dirname(os.path.abspath(path))
        os.makedirs(directory, exist_ok=True)
        # 独立的锁文件：os.replace 会更换账本文件的 inode，
        # 锁必须绑定在一个稳定的描述符上。
        lock_path = path + ".lock"
        self._lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as handle:
                self._state = self._upgrade(json.load(handle), total_epsilon, composition)
        else:
            self._state = self._initial_state(total_epsilon, composition)
            self._flush_locked(self._state)

    @staticmethod
    def _initial_state(total_epsilon: float, composition: str) -> dict[str, Any]:
        return {
                "total_epsilon": total_epsilon,
                "spent_epsilon": 0.0,
                "composition": composition,
                "versions": {},
                "releases": {},
                # 预算不足的拒绝决定也持久化（不扣预算），
                # 使“预算不足”在重开/重试后仍可与其他状态区分。
                "rejections": {},
            }

    @staticmethod
    def _upgrade(
        state: dict[str, Any], total_epsilon: float, composition: str
    ) -> dict[str, Any]:
        state.setdefault("rejections", {})
        for record in state.get("releases", {}).values():
            record.setdefault("delivered", False)
        return state

    def close(self) -> None:
        if self._lock_fd is not None:
            fd = self._lock_fd
            self._lock_fd = None
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def _read_latest(self, total_epsilon: float, composition: str) -> dict[str, Any]:
        """在持锁状态下重读磁盘，合并其他实例已提交的事务。"""
        if os.path.exists(self.path):
            with open(self.path, "r", encoding="utf-8") as handle:
                return self._upgrade(json.load(handle), total_epsilon, composition)
        return self._initial_state(total_epsilon, composition)

    @property
    def state(self) -> dict[str, Any]:
        if not self.in_memory:
            with self._file_lock():
                self._state = self._read_latest(
                    self._state["total_epsilon"], self._state["composition"]
                )
        return self._state

    def _file_lock(self):
        return _FileLock(self._lock_fd)

    def transaction(self, mutate) -> Any:
        """在跨实例排他临界区中执行 ``mutate(candidate)``。

        锁内先重读磁盘最新状态，再在深拷贝上变更并整体原子落盘；
        ``mutate`` 抛异常则放弃全部修改（余额与发布记录都不变）。
        先完成落盘再替换内存态：若落盘失败（如磁盘错误、进程被杀），
        内存与磁盘都保持提交前状态，不会出现“内存已扣账但磁盘丢失”。

        返回 ``mutate`` 的返回值。
        """
        if self.in_memory:
            with self._mem_lock:
                candidate = copy.deepcopy(self._state)
                result = mutate(candidate)
                self._state = candidate
                return result
        with self._file_lock():
            latest = self._read_latest(
                self._state["total_epsilon"], self._state["composition"]
            )
            candidate = copy.deepcopy(latest)
            result = mutate(candidate)
            self._flush_locked(candidate)
            self._state = candidate
            return result

    def commit(self, mutate) -> Any:
        """兼容旧名称：等价于 :meth:`transaction`。"""
        return self.transaction(mutate)

    def _flush_locked(self, state: dict[str, Any]) -> None:
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


class _FileLock:
    """``flock`` 排他锁上下文（跨线程、跨进程互斥）。"""

    def __init__(self, fd: int | None) -> None:
        self._fd = fd

    def __enter__(self) -> "_FileLock":
        if self._fd is not None:
            fcntl.flock(self._fd, fcntl.LOCK_EX)
        return self

    def __exit__(self, *exc: object) -> None:
        if self._fd is not None:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
