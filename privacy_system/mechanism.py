"""加噪机制。

第一版仅实现拉普拉斯机制（Laplace mechanism）：

* 计数查询的全局敏感度 Δf = 1（增删一个个体至多改变计数 1）；
* 对真实计数 C 加噪声 X ~ Laplace(0, Δf/ε)；
* 发布值 C' = C + X，满足 (ε, 0)-差分隐私（纯差分隐私，δ = 0）。
"""

from __future__ import annotations

import math
import random


def laplace_noise(rng: random.Random, sensitivity: float, epsilon: float) -> float:
    """用给定随机源生成一个 Laplace(0, sensitivity/epsilon) 噪声样本。

    随机源必须可注入（如 ``random.Random(固定种子)``），
    以便测试对记账与发布值做确定性核对。
    """
    if sensitivity <= 0:
        raise ValueError("sensitivity 必须为正数")
    if epsilon <= 0:
        raise ValueError("epsilon 必须为正数")
    scale = sensitivity / epsilon
    uniform = rng.random() - 0.5  # (-0.5, 0.5)
    sign = 1.0 if uniform >= 0 else -1.0
    return -scale * sign * math.log(1.0 - 2.0 * abs(uniform))
