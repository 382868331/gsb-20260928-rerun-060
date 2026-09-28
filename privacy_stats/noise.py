"""加噪随机源。

生产环境使用 :class:`OSRandomSource`（``secrets`` 的操作系统熵）；
测试使用 :class:`DeterministicRandomSource`（固定种子的 MT19937），
用于可复现地核对记账与发布结果。
"""

from __future__ import annotations

import math
import secrets
from abc import ABC, abstractmethod


class RandomSource(ABC):
    """加噪随机源接口：机制只能通过它取随机数。"""

    @abstractmethod
    def laplace(self, scale: float) -> float:
        """抽取一个尺度为 ``scale`` 的拉普拉斯随机变量。"""

    @abstractmethod
    def seed_state(self) -> str:
        """返回可记入审计日志的随机源状态描述。"""


class DeterministicRandomSource(RandomSource):
    """固定种子随机源（标准库 ``random.Random``，MT19937）。

    拉普拉斯噪声用逆 CDF 生成：``X = -scale * sgn(V) * ln(1 - 2|V|)``，
    ``V = U - 1/2``，``U`` 取自 ``Random.random``。
    给定种子与调用序列，结果完全可复现。
    """

    def __init__(self, seed: int):
        import random

        self._seed = int(seed)
        self._rng = random.Random(self._seed)
        self._calls = 0

    def laplace(self, scale: float) -> float:
        u = self._rng.random() - 0.5
        self._calls += 1
        sign = 1.0 if u >= 0 else -1.0
        return -scale * sign * math.log(1.0 - 2.0 * abs(u))

    def seed_state(self) -> str:
        return f"deterministic:mt19937:seed={self._seed}:calls={self._calls}"


class OSRandomSource(RandomSource):
    """生产随机源，基于操作系统熵按 CDF 逆变换生成拉普拉斯噪声。"""

    def laplace(self, scale: float) -> float:
        u = secrets.randbelow(2**53) / 2**53 - 0.5
        sign = 1.0 if u >= 0 else -1.0
        return -scale * sign * math.log(1.0 - 2.0 * abs(u))

    def seed_state(self) -> str:
        return "os:secrets:non-reproducible"
