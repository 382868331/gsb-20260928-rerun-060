# 隐私统计发布系统（第一版）

在虚构数据上提供**有限种计数发布**，并为每一次发布维护差分隐私预算账。
核心约束：**所有统计操作必须经过同一个预算管理入口
`PrivacyGateway.release()`，不存在绕过预算直接读取原始数据的公开路径。**

## 目录结构

- `privacy_system/fictional_data.py`：内置虚构示例数据（10 行，非真实个人信息）。
- `privacy_system/catalog.py`：允许发布的计数操作白名单（`count_all`、
  `count_by_city`、`count_by_age_group`、`count_by_membership`），每个操作登记敏感度。
- `privacy_system/mechanism.py`：拉普拉斯加噪机制（随机源可注入）。
- `privacy_system/ledger.py`：JSON 文件预算账，原子提交（临时文件 + fsync + rename）。
- `privacy_system/gateway.py`：唯一发布入口，负责版本、查重、预算校验与发布。
- `tests/test_privacy_system.py`：固定随机源的确定性测试。

## 加噪方法

采用**拉普拉斯机制**（满足 ε-差分隐私，δ = 0）：

- 计数查询的全局敏感度 Δf = 1（增删一个个体至多使计数变化 1）；
- 每次发布从 Lap(0, Δf/ε) 抽取噪声 X；
- 发布值 `value = 真实计数 + X`。
  **发布值是带噪估计量，不是真实精确数量**，对外接口（`ReleaseResult`、
  `list_releases()`、CLI 输出）只暴露带噪值，**不暴露真实计数，也不暴露本次噪声**——
  若同时给出带噪值与噪声，两者相减即可还原精确计数。
  真实计数与噪声仅保留在内部账本与 `audit_release()` 审计接口中，
  供内部审计核对，禁止向查询者输出。

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
| 相同 `request_id` + 相同版本/操作/参数 | 返回首次结果（`is_replay=True`） | 否 | 不重新加噪 |
| 相同 `request_id` 但参数不同 | 抛 `DuplicateRequest` 拒绝 | 否 | — |
| 不同 `request_id`（即使查询内容完全相同） | 独立的新发布 | 是 | 重新加噪 |

## 发布安排：预演、批准、整组或分阶段兑现

一组相关统计可以先预演，再由申请人明确批准。安排的统计身份由
**数据版本指纹 + 操作名 + 完整筛选参数**确定；显示名称（label）只用于展示，
改变筛选范围或统计含义会得到不同 stat_identity，不能因名称相同复用旧事实。

### 生命周期

1. preview_arrangement(items, mode=...) 只返回统计身份、当前可共享的既有发布、
   每项预算去向和整组是否可在当前余额下原子兑现。预演不落盘、不批准、不预留预算、
   不抽随机噪声，也不产生任何可领取结果。
2. approve_arrangement(arrangement_id, items, mode=...) 冻结批准时的数据范围、
   统计身份和 ε 去向；此时仍不产生结果、不扣预算。
3. MODE_ALL = "all_at_once" 调用 fulfill_arrangement()：在同一原子事务中
   检查整组新增预算。余额不足时全部拒绝，不产生任何新发布、不扣预算。
4. MODE_PHASED = "phased" 调用 fulfill_arrangement_item(arrangement_id, item_id)
   逐项兑现。已发布的事实和预算消耗永久保留；未发布部分可由
   cancel_arrangement() 取消。
5. claim_arrangement_item()/claim_arrangement() 只领取已经兑现的结果，重复领取返回
   同一份带噪结果且不重复扣费；重开后已领取状态仍可继续查询原结果。
   尚未兑现的统计不能领取，也不能借另一个安排的共享关系提前泄露。

### 交叠安排与共享

兑现时会在账本锁内重新查找相同 stat_identity 的既有发布事实：

- 只有已经提交、可公开共享的事实可复用；另一个安排仅批准但尚未兑现时不可提前引用。
- 复用既有事实的安排项 ε 为 0，审计事件指向 source_request_id，预算只由首次发布消耗。
- 多个安排争用余额时，先完成原子提交的安排消耗预算；另一个安排随后若余额不足会失败，
  但已经分阶段发布的项不会回滚。
- 重启后重新登记同一数据版本，账本继续使用批准时冻结的版本指纹和参数；新资料应注册新版本，
  重新研究并批准为独立安排。

公开安排视图只返回状态、身份、参数、来源请求 ID 与预算；公开审计
public_audit_log() 只给出发布/复用事件和 ε 变化，不返回精确统计、随机噪声或带噪值。

## 原子性与重启

每次发布把「预算余额增量」和「发布记录」放入同一次原子提交：
先在内存副本上完成校验与变更，再写临时文件、`fsync`、`os.replace` 原子替换。
因此发布成功与扣预算成功不可分割；进程重启后读回账本：

- 已扣预算与已发布结果仍然存在；
- 重放旧 `request_id` 只返回历史结果，**不会重复发放/重复扣账**；
- 余额不足时新查询依旧被拒绝。

原始数据不写入账本；重启后需用同一数据重新注册版本（带 SHA-256 指纹校验，
防止同一版本号绑定不同数据）。

## 并发一致性

「查重 → 版本/参数校验 → 预算校验 → 加噪 → 扣账 → 落盘」整体串行化：

- **同一进程内**用线程锁；**多个进程/发布实例共用同一份账本文件**时，
  通过账本旁的 `flock` 排他锁文件（`<db>.lock`）跨进程串行，
  并在锁内重新读盘。任何实例都基于磁盘最新事实做判断，
  不会用启动时的旧快照重复发放，也不会用整文件覆盖丢掉别的实例的提交。
- 并发到达的相同 `request_id`（即使来自不同实例）只有一个真正发布并扣预算，
  其余按重放返回**同一份**公开结果。
- 不同 `request_id` 争用最后一点预算时，恰好余额允许的个数成功，
  其余抛 `BudgetExhausted`，总额不会超过预算；重开新实例读回同一账本，
  公开输出（带噪值、已用预算、发布列表）与账本完全一致。

## 提交中途退出：查询与接续

一次请求在调用者视角有三种可观察状态，可用 `request_status(request_id)` 查询：

| 状态 | 含义 | 预算 |
|------|------|------|
| `not_released` | 账本中没有该请求，尚未发布 | 未扣 |
| `published_unacknowledged` | 已原子提交（结果与扣账都已落盘），但尚无证据调用者收到回执 | 已扣 |
| `published` | 已发布且调用者至少收到过一次回执 | 已扣 |

预算不足由 `release()` 抛 `BudgetExhausted` 表达：它不产生发布记录、不扣预算，
因此该请求随后查询仍是 `not_released`，与“已发布但未收到”严格区分。

结果在「提交成功后、返回调用者前」丢失（连接中断/进程退出）时，状态停留在
`published_unacknowledged`。调用者用同一 `request_id` 再次 `release()`，
或调用 `get_release(request_id)`，即可取回**同一份**带噪结果（重放、不二次扣预算、
不重新加噪），取回后状态转为 `published`。测试可用
`gw.fail_next_delivery(request_id)` 稳定复现该中断点。

## 公开面核对

`tests/test_privacy_system.py` 中的公开面核对测试**只使用公开接口输出**
（`ReleaseResult.to_dict()`、`list_releases()`、`budget()`、`list_versions()`、
异常信息与 CLI 输出），尝试用公开数值的各种加减组合还原真实计数，
验证无法直接还原，而不是仅检查某个字段名是否存在。

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
