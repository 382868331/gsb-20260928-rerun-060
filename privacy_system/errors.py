"""隐私发布系统的异常类型。"""


class PrivacySystemError(Exception):
    """所有系统错误的基类。"""


class BudgetExhausted(PrivacySystemError):
    """剩余隐私预算不足以支付本次发布。"""


class DuplicateRequest(PrivacySystemError):
    """请求 ID 已存在，但参数与首次请求不一致。"""


class UnknownVersion(PrivacySystemError):
    """引用了不存在的数据版本。"""


class UnknownOperation(PrivacySystemError):
    """操作未在隐私网关注册。"""
