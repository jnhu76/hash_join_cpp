# 📄 agent.md — C → C++ 重构验证框架（完整版 v3.0 · AI约束增强版）

---

# 🧠 0. TL;DR（执行摘要）

本系统用于验证：

> **C → C++ 重构是否仅为语义层修改（refactor），而非算法/行为改变**

并新增：

> **AI修改必须在“可验证 + 可约束 + 可回滚”范围内执行**

---

## 🔥 核心机制（升级版）

```text
输入生成
    ↓
双执行（C vs C++）
    ↓
行为对拍（Comparator）
    ↓
Invariant 校验
    ↓
性能分析
    ↓
覆盖率验证
    ↓
AI修改约束验证（NEW）
    ↓
报告输出
```

---

# 🎯 1. 核心原则（Design Principles）

---

## P1：黑盒优先（Black-box First）

只相信输入输出，不相信代码结构

---

## P2：可观察等价（Observational Equivalence）

```text
∀ input: Program_C(input) ≡ Program_CPP(input)
```

---

## P3：失败优先（Fail Fast）

发现不一致 → 立即记录 + 停止扩散

---

## P4：可复现（Determinism）

所有 fuzz 必须带 seed，所有输入必须落盘

---

## P5：低信任模型（Zero Trust Refactor）

> 默认已经改坏，除非被证明没改坏

---

## 🔴 P6：AI受限修改（NEW）

> AI 不是开发者，而是“受限 patch 工具”

必须满足：

```text
修改最小化 + 语义不变 + 可验证
```

---

# 🧱 2. 系统架构（Architecture）

```text
                +----------------------+
                |   Input Generator    |
                +----------+-----------+
                           |
                           v
        +------------------+------------------+
        |                                     |
+-------v--------+                   +--------v-------+
|   C Baseline   |                   |   C++ Refactor |
+-------+--------+                   +--------+-------+
        |                                     |
        +------------------+------------------+
                           |
                           v
                +----------+-----------+
                |   Comparator         |
                +----------+-----------+
                           |
        +------------------+------------------+
        |                                     |
+-------v--------+                   +--------v-------+
| Invariant      |                   | Perf Analyzer  |
+-------+--------+                   +--------+-------+
        |                                     |
        +------------------+------------------+
                           |
                           v
        +------------------+------------------+
        | AI Guardrail Layer (NEW)            |
        +------------------+------------------+
                           |
                           v
                +----------+-----------+
                | Report Engine        |
                +----------------------+
```

---

# 🔒 3. AI Guardrail Layer（新增核心模块）

---

## 🎯 目标

防止 AI：

* 偷偷改算法
* 顺手重构
* 引入新 bug
* 改变复杂度

---

## 🧩 3.1 修改范围限制（Scope Control）

AI 必须满足：

```text
仅允许修改：
- 指定文件
- 指定函数
- 指定行附近（±5行）

禁止：
- 跨文件修改
- 修改函数签名
- 修改数据结构
```

---

## 🧩 3.2 修改类型白名单（Allowed Ops）

```text
允许：
✔ 添加 if 判空
✔ 添加边界检查
✔ 添加 mutex / atomic（局部）
✔ 添加 assert
✔ 添加日志

禁止：
❌ 重构
❌ 替换算法
❌ 改循环结构
❌ 改控制流
❌ 引入新容器
```

---

## 🧩 3.3 语义不变验证（Semantic Lock）

AI 必须输出：

```text
1. 修改是否影响输出？
2. 是否改变时间复杂度？
3. 是否改变内存访问模式？
```

若无法保证：

```text
REJECT: semantic risk
```

---

## 🧩 3.4 修改计划机制（强制）

AI必须先输出：

```md
## 修改计划
- 修改位置：
- 修改类型：
- 是否影响语义：
```

未通过审核 → 不允许改代码

---

## 🧩 3.5 自动Diff约束

```bash
git diff
```

必须满足：

```text
- 修改行数 < 20
- 不允许新增函数
```

---

## 🧩 3.6 风险扫描（自动）

检测：

```text
- STL 引入
- new/delete 增加
- 锁数量增加
- 循环结构变化
```

---

# ⚙️ 4. 模块详细规范（原系统）

（以下保留你原有设计，只做增强）

---

## 🔹 4.1 Input Generator

（保持原样）

---

## 🔹 4.2 Executor

新增：

```text
必须支持 sanitizer：
- ASAN
- TSAN
- UBSAN
```

---

## 🔹 4.3 Comparator

新增：

```text
必须支持：
- crash 对比
- timeout 对比
```

---

## 🔹 4.4 Invariant Validator

新增规则：

```text
invariant 必须覆盖：
- 边界条件
- 空输入
- 异常路径
```

---

## 🔹 4.5 Perf Analyzer

新增：

```text
必须检测：
- cache miss（可选）
- 分支预测变化（可选）
```

---

## 🔹 4.6 Coverage Analyzer

新增：

```text
必须输出：
- 未覆盖函数列表
```

---

## 🔹 4.7 Report Engine

新增：

```md
## AI Modification Audit
- 修改行数
- 修改类型
- 风险等级

## Sanitizer Result
- ASAN: OK / FAIL
- TSAN: OK / FAIL
```

---

# 🔁 5. 标准执行 Pipeline（升级版）

---

## Step 0：AI生成修改计划（NEW）

```text
AI → Plan
Human/Verifier → Check
```

---

## Step 1：编译（带 sanitizer）

```bash
-fsanitize=address,thread,undefined
```

---

## Step 2：生成输入

---

## Step 3：执行对拍

---

## Step 4：Invariant

---

## Step 5：Sanitizer 检测（NEW）

---

## Step 6：性能

---

## Step 7：覆盖率

---

## Step 8：AI修改验证（NEW）

---

## Step 9：报告

---

# 🧪 6. 最小可运行实现（升级）

```python
for case in cases:
    r_old = run("./old", case.input)
    r_new = run("./new", case.input)

    if not compare(r_old, r_new):
        log_failure(case)

    if not invariant(case, r_new):
        log_invariant_fail(case)

run_sanitizer()

check_diff_size()

generate_report()
```

---

# ⚠️ 7. 高风险点（强化版）

---

## ❌ 并发模型改变（新增）

```text
mutex → atomic → lock-free
```

---

## ❌ 内存布局改变

```text
vector → pointer array
```

---

## ❌ 分支结构变化

```text
if → early return
```

---

## ❌ 生命周期变化

```text
raw pointer → smart pointer
```

---

# 🧭 8. 最终判定（升级）

---

## ✅ SAFE

* 行为一致
* invariant 全通过
* sanitizer 全通过
* diff 合规

---

## ⚠️ RISKY

* perf 下降
* 修改范围过大

---

## ❌ UNSAFE

* 行为不一致
* sanitizer 报错
* invariant 失败
* AI修改越界

---

# 🧱 9. 强制规则（强化）

---

## Rule 1

> 没有 fuzz ≈ 没有测试

---

## Rule 2

> 没有 invariant ≈ 没有理解算法

---

## Rule 3

> 没有 perf 分析 ≈ 默认改了算法

---

## Rule 4

> 覆盖率不足 → 结论无效

---

## 🔴 Rule 5（新增）

> AI 修改必须可解释，否则无效

---

## 🔴 Rule 6（新增）

> diff 超过 20 行 → 默认危险

---

# ⚓ 最终铁律（升级版）

> **“你不能通过阅读代码证明重构正确，你必须限制修改 + 用系统验证逼近正确。”**
