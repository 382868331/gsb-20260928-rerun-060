"""隐私统计发布系统：数据版本、有限计数查询与差分隐私预算账。

所有统计发布都必须经过 :class:`~privacy_system.gateway.PrivacyGateway`，
禁止绕过预算入口直接读取原始数据。
"""

from .errors import BudgetExhausted, DuplicateRequest, PrivacySystemError, UnknownVersion
from .gateway import PrivacyGateway, ReleaseResult
from .mechanism import laplace_noise

__all__ = [
    "PrivacyGateway",
    "ReleaseResult",
    "laplace_noise",
    "PrivacySystemError",
    "BudgetExhausted",
    "DuplicateRequest",
    "UnknownVersion",
]
