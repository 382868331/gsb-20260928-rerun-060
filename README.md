# 隐私统计发布系统（第一版）

在虚构数据上提供**有限种计数发布**，并为每一次发布维护差分隐私预算账。
核心约束：**所有统计操作必须经过同一个预算管理入口
`PrivacyGateway.release()`，不存在绕过预算直接读取原始数据的公开路径。**

## 目录结构

- `privacy_system/fictional_data.py`：内置虚构示例数据（10 行，非真实个人信息）。
- `privacy_system/catalog.py`：允许发布的计数操作白名单（`count_all`、
  `count_by_city`、`count_by_age_group`、`count_by_membership`），每个操作登记敏感度。
- `privacy_system/mechanism.py`：拉普拉斯加噪机制（随机源可注入）。
- `privacy_system/ledger.py`：公开 JSON 预算账与独立私有审计文件，带文件锁和原子提交。
- `privacy_system/gateway.py`：唯一发布入口，负责版本、查重、预算校验与发布。
- `tests/test_privacy_system.py`：固定随机源的确定性测试。

## 加噪方法

采用**拉普拉斯机制**（满足 ε-差分隐私，δ = 0）：

- 计数查询的全局敏感度 Δf = 1（增删一个个体至多使计数变化 1）；
- 每次发布从 Lap(0, Δf/ε) 抽取噪声 X；
- 发布值 value = 真实计数 + X。
  **发布值是带噪估计量，不是真实精确数量**；对外接口（ReleaseResult、
  list_releases()、可下载公开账本）只暴露带噪值和预算/机制元数据，
  不暴露本次噪声或真实计数。

## 公开资料与内部审计

- 公开资料包括单次响应、发布列表、预算信息、版本信息和公开 JSON 账本。
- 公开账本不保存 true_count 或 noise；旧账本若含这些字段，启动时会自动重写清除。
- 本次噪声和真实计数只写入独立的 `.private-audit.json` 文件，权限为 0600。
- 内部审计必须在受信进程和受控文件权限内进行；查询者只拿到公开资料时，
  无法通过“带噪值减本次噪声”直接还原精确计数。

随机源通过构造函数注入（`random.Random(固定种子)`），测试据此核对记账结果。

## 预算合成规则

采用**顺序组合（sequential composition）**：总预算 ε_total 固定，
每次发布支付 ε_i 后余额

```
ε_remaining = ε_total − Σ ε_i
```

- 剩余预算 < 本次请求 ε：**拒绝发布**，不产生结果、不扣预算（原子回滚）；
- δ 始终为 0（纯 DP，不使用高斯机制）。

## 重复请求与新查询

以调用方提供的 `request_id` 幂等键区分：

| 情形 | 行为 | 扣预算？ | 加噪 |
|------|------|----------|------|
| 相同 `request_id` + 相同版本/操作/参数/ε | 返回首次结果（`is_replay=True`） | 否 | 不重新加噪 |
| 相同 `request_id` 但参数不同 | 抛 `DuplicateRequest` 拒绝 | 否 | — |
| 不同 `request_id`（即使查询内容完全相同） | 独立的新发布 | 是 | 重新加噪 |

## 原子性与重启

每次发布把「预算余额增量」和「发布记录」放入同一次原子提交：
先在内存副本上完成校验与变更，再写临时文件、`fsync`、`os.replace` 原子替换。
提交由线程锁和文件锁保护，并发重复请求只有一个请求生成噪声并扣预算，
其余请求获得同一公开结果。因此发布成功与扣预算成功对应；进程重启后读回账本：

- 已扣预算与已发布结果仍然存在；
- 重放旧 `request_id` 只返回历史结果，**不会重复发放/重复扣账**；
- 余额不足时新查询依旧被拒绝。

原始数据不写入账本；重启后需用同一数据重新注册版本（带 SHA-256 指纹校验，
防止同一版本号绑定不同数据）。

## 后续扩展约束

新增任何统计操作时，必须：

1. 在 `privacy_system/catalog.py` 注册操作并声明其敏感度；
2. 只能通过 `PrivacyGateway.release()` 发布并支付预算；
3. 禁止新增直接返回原始行/精确统计量的接口。

## 运行方式

```bash
# 测试（固定随机源，确定性核对）
python3 -m unittest discover -s tests -v

# 命令行演示
python3 -m privacy_system.cli --db ledger.json --total-epsilon 10 --seed 20260929
```

## 编程示例

```python
import random
from privacy_system import PrivacyGateway, BudgetExhausted

with PrivacyGateway("ledger.json", total_epsilon=10.0,
                    rng=random.Random(20260929)) as gw:
    version = gw.register_fictional_v1()
    result = gw.release("req-001", version, "count_by_city",
                        {"city": "Beijing"}, epsilon=1.0)
    print(result.value)            # 带噪估计值，非真实精确数量
    print(gw.budget())             # 总/已用/剩余 ε
    gw.release("req-001", version, "count_by_city",
               {"city": "Beijing"}, epsilon=1.0)  # 重放，is_replay=True，不扣预算
```
