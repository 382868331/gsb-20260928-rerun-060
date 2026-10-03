"""JSON 文件预算账本：发布记录与预算余额原子落盘。

每次提交都把“余额 + 发布记录”作为一个整体写入临时文件，
``flush`` + ``fsync`` 后用 :func:`os.replace` 原子替换，
因此发布结果和扣预算要么同时生效，要么都不生效；
进程崩溃或重启后不会出现“已发布但未扣账”的重复发放。

多个进程/网关实例可能共用同一份账本文件。所有“读入最新状态 →
校验 → 修改 → 落盘”的序列都在同一把跨进程文件锁（``flock``）内完成，
并在锁内重新读盘，因此不同实例不会基于各自启动时的旧快照
重复发放或把对方的提交覆盖丢失。
"""

from __future__ import annotations

import json
import os
import tempfile
import threading
from contextlib import contextmanager
from typing import Any

try:  # pragma: no cover - 平台分支
    import fcntl

    _HAVE_FCNTL = True
except ImportError:  # pragma: no cover - 非 POSIX 平台
    fcntl = None  # type: ignore[assignment]
    _HAVE_FCNTL = False


class FileLedger:
    def __init__(self, path: str, total_epsilon: float, composition: str) -> None:
        self.path = path
        self.in_memory = path == ":memory:"
        # 进程内锁：同一进程的多个线程共用一把文件锁描述符时先在此串行。
        self._thread_lock = threading.RLock()
        if self.in_memory:
            self._state: dict[str, Any] = {
                "total_epsilon": total_epsilon,
                "spent_epsilon": 0.0,
                "composition": composition,
                "versions": {},
                "releases": {},
                "arrangements": {},
                "budget_events": [],
            }
            return
        directory = os.path.dirname(os.path.abspath(path))
        os.makedirs(directory, exist_ok=True)
        # 独立的锁文件：账本 JSON 用 os.replace 原地替换，不能对其本身持锁。
        self._lock_path = path + ".lock"
        self._lock_handle = open(self._lock_path, "a+b")
        with self._locked():
            if os.path.exists(path):
                self._state = self._read_disk()
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
                "arrangements": {},
                "budget_events": [],
            }

    def _read_disk(self) -> dict[str, Any]:
        with open(self.path, "r", encoding="utf-8") as handle:
            state = json.load(handle)
        state.setdefault("arrangements", {})
        state.setdefault("budget_events", [])
        return state

    @contextmanager
    def _locked(self):
        """持有进程内锁；文件账本同时持有跨进程排他锁并在读入最新磁盘态。

        不可嵌套用于文件账本（内层释放会提前放开 flock）；
        网关侧的关键校验全部放在 :meth:`commit` 的 ``mutate`` 闭包中。
        """
        with self._thread_lock:
            if self.in_memory or not _HAVE_FCNTL:
                yield
                return
            fcntl.flock(self._lock_handle.fileno(), fcntl.LOCK_EX)
            try:
                if os.path.exists(self.path):
                    self._state = self._read_disk()
                yield
            finally:
                fcntl.flock(self._lock_handle.fileno(), fcntl.LOCK_UN)

    @property
    def state(self) -> dict[str, Any]:
        """账本当前状态。文件账本会先取锁并重新读盘，避免返回跨实例旧快照。"""
        if not self.in_memory and _HAVE_FCNTL:
            with self._locked():
                return self._state
        return self._state

    def snapshot(self) -> dict[str, Any]:
        """同 :attr:`state`，语义化命名：取一份可读取的最新账本视图。"""
        return self.state

    def close(self) -> None:
        """释放跨进程锁文件描述符（内存账本为空操作）。"""
        if self.in_memory or getattr(self, "_closed", False):
            return
        self._closed = True
        try:
            if _HAVE_FCNTL:
                fcntl.flock(self._lock_handle.fileno(), fcntl.LOCK_UN)
        finally:
            self._lock_handle.close()

    def commit(self, mutate) -> Any:
        """在一份深拷贝上执行变更；成功后整体原子落盘。

        ``mutate`` 抛异常则放弃全部修改（余额与发布记录都不变）。
        先完成落盘再替换内存态：若落盘失败（如磁盘错误、进程被杀），
        内存与磁盘都保持提交前状态，不会出现“内存已扣账但磁盘丢失”。
        """
        import copy

        with self._locked():
            # 锁内已重新读盘，candidate 基于磁盘上的最新事实，
            # 查重与预算校验若放在闭包外会被其他实例的并发提交击穿。
            candidate = copy.deepcopy(self._state)
            result = mutate(candidate)
            if not self.in_memory:
                self._flush_locked(candidate)
            self._state = candidate
            return result

    def _flush_locked(self, state: dict[str, Any]) -> None:
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
