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
import threading
from typing import Any

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows 不需要支持当前运行环境
    fcntl = None


def _scrub_public_state(state: dict[str, Any]) -> dict[str, Any]:
    """移除历史版本可能写入公开账本的敏感还原字段。"""
    clean = json.loads(json.dumps(state, ensure_ascii=False))
    for record in clean.get("releases", {}).values():
        record.pop("true_count", None)
        record.pop("noise", None)
    return clean


class AuditLog:
    """仅供内部审计使用的证据文件，默认不进入公开导出物。"""

    def __init__(self, path: str | None) -> None:
        self.path = path
        self.in_memory = path is None
        self._lock = threading.RLock()
        self._lock_fd: int | None = None
        if self.in_memory:
            self._state: dict[str, Any] = {"releases": {}}
            return
        lock_path = f"{path}.lock"
        os.makedirs(os.path.dirname(os.path.abspath(lock_path)), exist_ok=True)
        self._lock_fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        if fcntl is not None:
            fcntl.flock(self._lock_fd, fcntl.LOCK_EX)
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as handle:
                self._state = json.load(handle)
        else:
            self._state = {"releases": {}}
            self._flush()
        if fcntl is not None:
            fcntl.flock(self._lock_fd, fcntl.LOCK_UN)

    def _locked(self):
        class Locked:
            def __enter__(inner_self) -> "AuditLog":
                self._lock.acquire()
                if fcntl is not None and self._lock_fd is not None:
                    fcntl.flock(self._lock_fd, fcntl.LOCK_EX)
                return self

            def __exit__(inner_self, exc_type, exc, tb) -> None:
                if fcntl is not None and self._lock_fd is not None:
                    fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
                self._lock.release()

        return Locked()

    def _reload(self) -> None:
        if self.in_memory or not os.path.exists(self.path):
            return
        with open(self.path, "r", encoding="utf-8") as handle:
            self._state = json.load(handle)

    @property
    def state(self) -> dict[str, Any]:
        with self._locked():
            self._reload()
            return json.loads(json.dumps(self._state, ensure_ascii=False))

    def put_release(self, record: dict[str, Any]) -> None:
        with self._locked():
            self._reload()
            self._state.setdefault("releases", {})[record["request_id"]] = record
            if not self.in_memory:
                self._flush()

    def remove_release(self, request_id: str) -> None:
        with self._locked():
            self._reload()
            if request_id in self._state.get("releases", {}):
                del self._state["releases"][request_id]
                if not self.in_memory:
                    self._flush()

    def close(self) -> None:
        if self._lock_fd is not None:
            os.close(self._lock_fd)
            self._lock_fd = None

    def _flush(self) -> None:
        directory = os.path.dirname(os.path.abspath(self.path))
        fd, tmp_name = tempfile.mkstemp(prefix=".audit-", suffix=".tmp", dir=directory)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(self._state, handle, ensure_ascii=False, indent=2, sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(tmp_name, 0o600)
            os.replace(tmp_name, self.path)
            os.chmod(self.path, 0o600)
        except BaseException:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
            raise


class FileLedger:
    def __init__(self, path: str, total_epsilon: float, composition: str) -> None:
        self.path = path
        self.in_memory = path == ":memory:"
        self._lock = threading.RLock()
        self._lock_fd: int | None = None
        if self.in_memory:
            self._state: dict[str, Any] = {
                "total_epsilon": total_epsilon,
                "spent_epsilon": 0.0,
                "composition": composition,
                "versions": {},
                "releases": {},
            }
            return
        if not self.in_memory:
            lock_path = f"{path}.lock"
            directory = os.path.dirname(os.path.abspath(path))
            os.makedirs(directory, exist_ok=True)
            self._lock_fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
            if fcntl is not None:
                fcntl.flock(self._lock_fd, fcntl.LOCK_EX)
        try:
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as handle:
                    self._state = _scrub_public_state(json.load(handle))
                self._flush()
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
        finally:
            if fcntl is not None:
                fcntl.flock(self._lock_fd, fcntl.LOCK_UN)

    @property
    def state(self) -> dict[str, Any]:
        if not self.in_memory:
            with self._locked():
                self._reload()
                return json.loads(json.dumps(self._state, ensure_ascii=False))
        return self._state

    def _locked(self):
        class Locked:
            def __enter__(inner_self) -> "FileLedger":
                self._lock.acquire()
                if fcntl is not None and self._lock_fd is not None:
                    fcntl.flock(self._lock_fd, fcntl.LOCK_EX)
                return self

            def __exit__(inner_self, exc_type, exc, tb) -> None:
                if fcntl is not None and self._lock_fd is not None:
                    fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
                self._lock.release()

        return Locked()

    def _reload(self) -> None:
        if self.in_memory or not os.path.exists(self.path):
            return
        with open(self.path, "r", encoding="utf-8") as handle:
            self._state = _scrub_public_state(json.load(handle))

    def commit(self, mutate) -> Any:
        """在一份深拷贝上执行变更；成功后整体原子落盘。

        ``mutate`` 抛异常则放弃全部修改（余额与发布记录都不变）。
        """
        import copy

        with self._locked():
            self._reload()
            candidate = copy.deepcopy(self._state)
            result = mutate(candidate)
            try:
                self._state = candidate
                if not self.in_memory:
                    self._flush()
                return result
            except BaseException:
                self._reload()
                raise

    def close(self) -> None:
        if self._lock_fd is not None:
            os.close(self._lock_fd)
            self._lock_fd = None

    def _flush(self) -> None:
        directory = os.path.dirname(os.path.abspath(self.path))
        fd, tmp_name = tempfile.mkstemp(prefix=".ledger-", suffix=".tmp", dir=directory)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(self._state, handle, ensure_ascii=False, indent=2, sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(tmp_name, 0o644)
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
