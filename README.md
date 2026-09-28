# 隐私统计发布系统（第一版）

一个基于差分隐私的计数发布原型：原始数据分版本登记，所有统计发布经过
唯一入口，epsilon 预算由统一预算账扣除，结果带噪并明确标注“非精确数量”。

## 加噪方法与隐私参数

- 相邻数据集：相差至多一条记录（增/删一个人）。
- 第一版只注册两种计数机制（计数敏感度恒为 L1=1）：
  - total_count：总行数；
  - predicate_count：某列等于给定值的行数。
- 机制：拉普拉斯机制，噪声 X ~ Lap(scale = Δf/ε) = Lap(1/ε)；
  每次发布固定 ε=1.0，δ=0（纯 DP）。带噪值四舍五入为整数展示，
  内部同时保留未取整的浮点带噪值。
- 随机源：生产用 OSRandomSource（secrets 系统熵）；
  测试用 DeterministicRandomSource（固定种子 MT19937 + 拉普拉斯逆 CDF），
  结果可复现核对。

## 预算合成规则

- 纯 DP 拉普拉斯机制按线性（串行）组合定理合成：
  ε_total = Σ ε_i，δ_total = 0。
- 不使用 advanced composition（需要固定轮数并引入 δ，与纯 DP 账不符）。
- 全局单一 ε 预算池，开户时设定、不可上调；余额不足则整笔拒绝，不发结果。

## 重复请求与新查询

- 每个请求带客户端提供的 request_id：
  - 相同 request_id 且相同版本/机制/参数：幂等返回首次结果，
    不再重新加噪、不再扣费（reused=True, epsilon_spent=0）。
  - request_id 相同但查询不同：拒绝，防止幂等键被顶替成别的查询。
  - 新 request_id（或新版本上的查询）：新发布，正常扣 ε。
- 数据版本按内容哈希去重：同样的数据重复导入得到同一 version_id，
    便于区分“同一版本上的重复请求”和“新版本上的新查询”。

## 原子性与重启语义

- 发布与扣费在同一个 SQLite 事务（BEGIN IMMEDIATE）内完成：
  校验预算 → 读真实值 → 加噪 → 写 releases 表 → 条件 UPDATE 扣预算
  （WHERE remaining_e >= ε，compare-and-deduct，防超支）。
  任何一步失败整体回滚：不会出现“发了结果没扣费”或“扣了费没结果”。
- 状态全部持久化在 SQLite（WAL）。进程重启后重放旧 request_id 会命中既有
  发布记录，不会重复发放、不会重复扣费。

## 统一入口约束

- 原始行只能通过 DataVault 读取，而 vault 只在 Publisher 执行
  已注册机制时使用；未注册的统计名直接报错，无法绕过预算读原数据。
- 后续新增统计操作：在 privacy_stats/mechanisms.py 注册机制
  （声明 ε 与敏感度），无需改动预算与事务逻辑。

## 运行

    python3 examples/run_demo.py
    python3 -m unittest discover -s tests -v

注意：结果对象中的 noisy_result 是加噪估计，privacy_note 固定声明
它不是真实精确数量；真实计数只存在于机制执行栈内，不落库、不外发。
