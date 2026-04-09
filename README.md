# Radix Hash Join - C++ Implementation

高性能基数哈希连接算法的 C++20 实现，支持多线程并行处理和查询优化。

## 项目简介

本项目是 C 语言实现的基数哈希连接算法的 C++20 重构版本，旨在：
- 利用现代 C++ 特性提升代码安全性和可维护性
- 保持与原始 C 实现的核心算法一致性
- 提供多种编译模式支持代码分析和性能调优
- 支持内存检测、性能分析和覆盖率分析

## 核心特性

- **基数哈希连接算法**：高效的并行哈希连接实现
- **多线程支持**：基于线程池的任务调度
- **查询优化器**：基于统计信息的查询计划选择
- **现代 C++20**：使用智能指针、STL 容器、RAII 等特性
- **多种编译模式**：支持调试、发布、内存检测、性能分析等模式
- **验证框架**：完整的 C → C++ 重构验证系统

## 快速开始

### 环境要求

- C++20 兼容编译器 (GCC 10+, Clang 12+)
- Xmake 构建系统
- Linux 操作系统
- (可选) Valgrind 用于内存检测

### 安装依赖

```bash
# Ubuntu/Debian
sudo apt update
sudo apt install g++ clang valgrind xmake

# 安装 Xmake (如果未安装)
curl -fsSL https://xmake.io/shget.text | bash
```

### 基础编译和运行

```bash
# 1. 配置为发布模式
xmake f -m release

# 2. 编译项目
xmake build

# 3. 运行程序
cd workloads/small
(cat small.init; echo "Done"; cat small.work) | ../../build/linux/x86_64/release/Driver
```

## 编译模式详解

本项目支持多种编译模式，用于不同的开发和分析场景：

### 1. 基础模式

| 模式 | 命令 | 用途 |
|------|------|------|
| **Release** | `xmake f -m release` | 生产环境，最大性能优化 |
| **Debug** | `xmake f -m debug` | 开发调试，带符号表 |

### 2. 内存检测模式

#### Valgrind 模式 (推荐用于内存泄漏检测)
```bash
# 配置并编译
xmake f -m valgrind && xmake build

# 运行 Valgrind 内存检测
xmake run valgrind_run

# 使用 workload 运行 Valgrind
xmake run valgrind_workload

# 深度内存检测（更详细的检查）
xmake run memcheck
```

#### AddressSanitizer 模式 (更快的内存检测)
```bash
# 配置并编译
xmake f -m asan && xmake build

# 直接运行，ASan 会自动检测内存错误
xmake run Driver
```

#### 其他 Sanitizer 模式
```bash
# ThreadSanitizer - 检测数据竞争
xmake f -m tsan && xmake build

# LeakSanitizer - 检测内存泄漏
xmake f -m lsan && xmake build

# UndefinedBehaviorSanitizer - 检测未定义行为
xmake f -m ubsan && xmake build

# 综合检测模式
xmake f -m check && xmake build
```

### 3. 性能分析模式

#### gprof 性能分析
```bash
# 配置并编译
xmake f -m profile && xmake build

# 运行程序生成 gmon.out
xmake run Driver

# 查看性能分析报告
gprof build/linux/x86_64/profile/Driver gmon.out > profile_report.txt
```

### 4. 代码覆盖率模式
```bash
# 配置并编译
xmake f -m coverage && xmake build

# 运行测试
xmake run optimizer_test

# 生成覆盖率报告
gcov src/*.cpp
lcov --capture --directory . --output-file coverage.info
genhtml coverage.info --output-directory coverage_report
```

## 验证框架 (Verification Framework)

本项目包含完整的 C → C++ 重构验证框架，用于确保重构后的代码与原始 C 实现行为一致。

### 验证框架概述

验证框架基于 `agent.md` 规范实现，核心原则：
- **黑盒优先**：以输入输出为第一判断标准
- **可观察等价**：C 和 C++ 版本对相同输入产生相同输出
- **失败优先**：一旦发现不一致立即记录
- **可复现**：所有测试必须可复现
- **低信任模型**：默认已经改坏了算法，除非被证明没改坏

### 运行验证框架

```bash
# 进入项目目录
cd /home/hoo/Source/hashjoin/final_cpp

# 运行完整验证（需要先编译 C 和 C++ 版本）
python3 verification/verify_refactor.py \
    --c-binary ../final/build/release/Driver \
    --cpp-binary ./build/linux/x86_64/release/Driver \
    --workloads ./workloads/small \
    --fuzz-count 100

# 查看验证报告
cat verification/reports/verification_report_*.md
```

### 验证框架组件

| 组件 | 功能 | 输出 |
|------|------|------|
| **Input Generator** | 生成测试用例 | 确定性测试、边界测试、模糊测试 |
| **Executor** | 执行二进制文件 | stdout, stderr, exit_code, 执行时间 |
| **Comparator** | 比较 C 和 C++ 结果 | 差异报告 |
| **Invariant Validator** | 验证算法不变量 | 格式检查、崩溃检测、内存错误检测 |
| **Perf Analyzer** | 性能对比分析 | 执行时间比率、性能判定 |
| **Report Engine** | 生成验证报告 | Markdown + JSON 格式报告 |

### 验证判定标准

| 判定 | 条件 |
|------|------|
| **✅ SAFE** | 全部一致，invariant 全通过，perf 正常，覆盖率达标 |
| **⚠️ RISKY** | 少量失败，或性能下降 > 1.3x |
| **❌ UNSAFE** | 行为不一致，invariant 失败，或性能下降 > 1.5x |

### 验证示例

```bash
# 快速验证（10个模糊测试用例）
python3 verification/verify_refactor.py --fuzz-count 10

# 完整验证（100个模糊测试用例）
python3 verification/verify_refactor.py --fuzz-count 100

# 指定特定 workload
python3 verification/verify_refactor.py --workloads ./workloads/small
```

## 项目结构

```
final_cpp/
├── include/           # 头文件
│   ├── Build.hpp
│   ├── Intermediate.hpp
│   ├── JobScheduler.hpp
│   ├── Joiner.hpp
│   ├── Operations.hpp
│   ├── Optimizer.hpp
│   ├── Parser.hpp
│   ├── Partition.hpp
│   ├── Probe.hpp
│   ├── Queue.hpp
│   ├── Relation.hpp
│   ├── Utils.hpp
│   └── Vector.hpp
├── src/              # 源文件
│   ├── Build.cpp
│   ├── Intermediate.cpp
│   ├── JobScheduler.cpp
│   ├── Joiner.cpp
│   ├── main.cpp
│   ├── Operations.cpp
│   ├── Optimizer.cpp
│   ├── Parser.cpp
│   ├── Partition.cpp
│   ├── Probe.cpp
│   ├── Queue.cpp
│   ├── Relation.cpp
│   └── Vector.cpp
├── tests/            # 测试文件
│   └── optimizer_test.cpp
├── workloads/        # 测试数据
│   └── small/
│       ├── small.init
│       ├── small.work
│       └── r0, r1, r2, r3 (关系数据文件)
├── verification/     # 验证框架
│   ├── verify_refactor.py
│   ├── corpus/
│   └── reports/
├── xmake.lua         # 构建配置
├── .gitignore        # Git 忽略文件
└── README.md         # 本文件
```

## 常用命令速查

### 编译相关
```bash
# 基础编译
xmake f -m release && xmake build

# 调试编译
xmake f -m debug && xmake build

# 清理构建
xmake clean

# 重新配置
xmake f -c
```

### 运行相关
```bash
# 运行主程序
xmake run Driver

# 运行测试
xmake run optimizer_test

# 运行 harness
xmake run harness
```

### 内存检测
```bash
# Valgrind 模式编译 + 运行
xmake f -m valgrind && xmake build
xmake run valgrind_run

# AddressSanitizer 模式
xmake f -m asan && xmake build
xmake run Driver
```

### 验证框架
```bash
# 运行验证
python3 verification/verify_refactor.py --fuzz-count 100

# 查看报告
cat verification/reports/verification_report_*.md
```

## 性能对比

验证框架会自动对比 C 和 C++ 版本的性能：

```bash
# 运行性能对比
python3 verification/verify_refactor.py --fuzz-count 10

# 查看性能报告
# 报告中会显示：
# - C 版本执行时间
# - C++ 版本执行时间
# - 性能比率 (C++ / C)
# - 判定结果 (OK / WARN / FAIL)
```

性能判定标准：
- **OK**: 比率 < 1.2 (性能相当)
- **WARN**: 1.2 ≤ 比率 < 1.5 (性能略有下降)
- **FAIL**: 比率 ≥ 1.5 (性能严重下降)

## 注意事项

1. **编译器选择**：默认使用系统默认编译器，可通过 `xmake f --toolchain=clang` 切换
2. **内存检测**：Valgrind 模式编译的程序运行较慢，属正常现象
3. **测试数据**：运行程序需要配合 workload 目录中的 `.init` 和 `.work` 文件
4. **验证框架**：需要同时编译 C 版本（在 `../final/` 目录）和 C++ 版本

## 贡献

本项目是基于 C 语言实现的基数哈希连接算法的 C++20 重构，主要改进包括：
- 使用现代 C++ 特性（智能指针、STL 容器、RAII）
- 修复原始 C 代码中的内存泄漏和未定义行为
- 添加完整的内存检测和验证框架
- 优化线程同步机制

## 许可证

[待添加]

## 联系方式

[待添加]
