#!/usr/bin/env python3
"""
C → C++ 重构验证框架
根据 agent.md 规范实现完整的验证流程

核心原则：
1. 黑盒优先 - 以输入输出为第一判断标准
2. 可观察等价 - C 和 C++ 版本对相同输入产生相同输出
3. 失败优先 - 一旦发现不一致立即记录
4. 可复现 - 所有测试必须可复现
5. 低信任模型 - 默认已经改坏了算法
"""

import json
import os
import random
import subprocess
import time
import sys
import shutil
import hashlib
import statistics
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Optional, Tuple, Any
from datetime import datetime
import argparse


# =============================================================================
# 配置
# =============================================================================

@dataclass
class Config:
    """验证框架配置"""
    c_binary: str = "../final/Driver"
    cpp_binary: str = "./build/linux/x86_64/debug/Driver"
    workloads_dir: str = "./workloads/small"
    corpus_dir: str = "./verification/corpus"
    reports_dir: str = "./verification/reports"
    timeout_seconds: int = 10
    max_input_size: int = 1024 * 1024  # 1MB
    fuzz_count: int = 100
    fuzz_seed: int = 42
    perf_iterations: int = 5
    perf_threshold_warn: float = 1.2
    perf_threshold_fail: float = 1.5
    coverage_threshold: float = 0.7


# =============================================================================
# 数据结构
# =============================================================================

@dataclass
class TestCase:
    """测试用例"""
    id: str
    name: str
    category: str  # deterministic, edge, fuzz
    seed: Optional[int] = None
    input_file: Optional[str] = None
    input_content: Optional[str] = None
    description: str = ""


@dataclass
class ExecutionResult:
    """执行结果"""
    stdout: str
    stderr: str
    exit_code: int
    duration_ms: float
    crash: bool
    timeout: bool
    memory_bytes: int = 0


@dataclass
class ComparisonResult:
    """比较结果"""
    passed: bool
    stdout_match: bool
    stderr_match: bool
    exit_code_match: bool
    diff_stdout: str = ""
    diff_stderr: str = ""


@dataclass
class InvariantResult:
    """不变量验证结果"""
    passed: bool
    name: str
    message: str = ""


@dataclass
class PerformanceResult:
    """性能分析结果"""
    c_time_ms: float
    cpp_time_ms: float
    ratio: float
    verdict: str  # OK, WARN, FAIL


@dataclass
class VerificationReport:
    """验证报告"""
    timestamp: str
    total_cases: int = 0
    passed: int = 0
    failed: int = 0
    crashes_c: int = 0
    crashes_cpp: int = 0
    timeouts: int = 0
    invariant_failures: int = 0
    performance_issues: int = 0
    coverage_line: float = 0.0
    coverage_branch: float = 0.0
    verdict: str = "UNKNOWN"
    failures: List[Dict] = field(default_factory=list)
    performance_results: List[Dict] = field(default_factory=list)


# =============================================================================
# 3.1 Input Generator（输入生成器）
# =============================================================================

class InputGenerator:
    """输入生成器 - 最大化覆盖输入空间"""
    
    def __init__(self, config: Config):
        self.config = config
        self.corpus_dir = Path(config.corpus_dir)
        self.workloads_dir = Path(config.workloads_dir)
    
    def generate_all(self) -> List[TestCase]:
        """生成所有测试用例"""
        cases = []
        
        # 1. 确定性测试（使用现有 workload）
        cases.extend(self._generate_deterministic())
        
        # 2. 边界测试
        cases.extend(self._generate_edge_cases())
        
        # 3. 模糊测试
        cases.extend(self._generate_fuzz())
        
        return cases
    
    def _generate_deterministic(self) -> List[TestCase]:
        """确定性测试 - 使用现有的 workload 文件"""
        cases = []
        workloads_path = self.workloads_dir
        
        if not workloads_path.exists():
            return cases
        
        # 查找所有 .init 文件（关系文件列表）
        for init_file in sorted(workloads_path.glob("*.init")):
            # 查找对应的 .work 文件
            work_file = init_file.with_suffix('.work')
            if work_file.exists():
                # 合并 init + "Done" + work 文件内容
                init_content = init_file.read_text()
                work_content = work_file.read_text()
                # 程序需要 "Done" 来结束关系文件输入
                combined_content = init_content + "Done\n" + work_content
                
                case = TestCase(
                    id=f"det_{init_file.stem}",
                    name=init_file.stem,
                    category="deterministic",
                    input_content=combined_content,
                    description=f"Workload: {init_file.stem}"
                )
                cases.append(case)
        
        return cases
    
    def _generate_edge_cases(self) -> List[TestCase]:
        """边界测试用例"""
        cases = []
        edge_dir = self.corpus_dir / "edge"
        edge_dir.mkdir(parents=True, exist_ok=True)
        
        # 1. 空输入
        empty_case = TestCase(
            id="edge_empty",
            name="empty_input",
            category="edge",
            description="Empty input"
        )
        empty_file = edge_dir / "empty.work"
        empty_file.write_text("")
        empty_case.input_file = str(empty_file)
        cases.append(empty_case)
        
        # 2. 单行输入
        single_case = TestCase(
            id="edge_single",
            name="single_line",
            category="edge",
            description="Single line input"
        )
        single_file = edge_dir / "single.work"
        single_file.write_text("0\nDone\n")
        single_case.input_file = str(single_file)
        cases.append(single_case)
        
        # 3. 极大关系ID
        max_id_case = TestCase(
            id="edge_max_id",
            name="max_relation_id",
            category="edge",
            description="Maximum relation ID"
        )
        max_id_file = edge_dir / "max_id.work"
        max_id_file.write_text("999999\nDone\n")
        max_id_case.input_file = str(max_id_file)
        cases.append(max_id_case)
        
        # 4. 重复数据
        dup_case = TestCase(
            id="edge_duplicate",
            name="duplicate_data",
            category="edge",
            description="Duplicate data in relations"
        )
        dup_file = edge_dir / "duplicate.work"
        # 创建一个简单的重复数据 workload
        dup_content = "0\n0.0=0.0\nDone\n"
        dup_file.write_text(dup_content)
        dup_case.input_file = str(dup_file)
        cases.append(dup_case)
        
        return cases
    
    def _generate_fuzz(self) -> List[TestCase]:
        """模糊测试用例"""
        cases = []
        random.seed(self.config.fuzz_seed)
        
        for i in range(self.config.fuzz_count):
            seed = random.randint(0, 2**32 - 1)
            case = TestCase(
                id=f"fuzz_{i:04d}",
                name=f"fuzz_case_{i}",
                category="fuzz",
                seed=seed,
                description=f"Fuzz test with seed {seed}"
            )
            
            # 生成模糊输入
            input_content = self._generate_fuzz_input(seed)
            case.input_content = input_content
            
            cases.append(case)
        
        return cases
    
    def _generate_fuzz_input(self, seed: int) -> str:
        """生成模糊输入"""
        random.seed(seed)
        
        # 随机选择输入类型
        input_type = random.choice(['simple', 'complex', 'malformed'])
        
        if input_type == 'simple':
            # 简单的查询格式
            num_relations = random.randint(0, 5)
            relations = "\n".join(str(random.randint(0, 100)) for _ in range(num_relations))
            return f"{relations}\nDone\n"
        
        elif input_type == 'complex':
            # 复杂的查询格式
            lines = []
            num_relations = random.randint(1, 10)
            lines.append(" ".join(str(random.randint(0, 50)) for _ in range(num_relations)))
            
            # 添加谓词
            num_predicates = random.randint(0, 5)
            predicates = []
            for _ in range(num_predicates):
                rel = random.randint(0, num_relations - 1)
                col = random.randint(0, 3)
                op = random.choice(['=', '<', '>', '<=', '>='])
                val = random.randint(0, 1000)
                predicates.append(f"{rel}.{col}{op}{val}")
            
            if predicates:
                lines.append("&".join(predicates))
            
            # 添加选择项
            num_selects = random.randint(1, 3)
            selects = []
            for _ in range(num_selects):
                rel = random.randint(0, num_relations - 1)
                col = random.randint(0, 3)
                selects.append(f"{rel}.{col}")
            lines.append(" ".join(selects))
            
            return "\n".join(lines) + "\n"
        
        else:
            # 格式错误的输入
            length = random.randint(0, 100)
            chars = "0123456789abcdefABCDEF \n\t|&.=<>!@#$%^&*()"
            return ''.join(random.choice(chars) for _ in range(length))


# =============================================================================
# 3.2 Executor（执行器）
# =============================================================================

class Executor:
    """执行器 - 运行二进制文件并捕获结果"""
    
    def __init__(self, config: Config):
        self.config = config
    
    def run(self, binary: str, test_case: TestCase, workloads_dir: str = None) -> ExecutionResult:
        """执行测试用例"""
        start_time = time.time()
        
        # 准备输入
        input_data = None
        cwd = None
        
        if test_case.input_file:
            # 从文件读取输入
            input_path = Path(test_case.input_file)
            if input_path.exists():
                input_data = input_path.read_text()
                # 设置工作目录为 workload 所在目录
                if workloads_dir:
                    cwd = workloads_dir
                else:
                    cwd = str(input_path.parent)
        elif test_case.input_content:
            input_data = test_case.input_content
            cwd = self.config.workloads_dir
        
        if input_data is None:
            input_data = ""
        
        try:
            # 执行命令
            result = subprocess.run(
                [binary],
                input=input_data,
                capture_output=True,
                text=True,
                timeout=self.config.timeout_seconds,
                cwd=cwd
            )
            
            duration_ms = (time.time() - start_time) * 1000
            
            return ExecutionResult(
                stdout=result.stdout,
                stderr=result.stderr,
                exit_code=result.returncode,
                duration_ms=duration_ms,
                crash=False,
                timeout=False
            )
        
        except subprocess.TimeoutExpired:
            duration_ms = self.config.timeout_seconds * 1000
            return ExecutionResult(
                stdout="",
                stderr="TIMEOUT",
                exit_code=-1,
                duration_ms=duration_ms,
                crash=False,
                timeout=True
            )
        
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            return ExecutionResult(
                stdout="",
                stderr=str(e),
                exit_code=-1,
                duration_ms=duration_ms,
                crash=True,
                timeout=False
            )


# =============================================================================
# 3.3 Comparator（对拍模块）
# =============================================================================

class Comparator:
    """对拍模块 - 比较 C 和 C++ 执行结果"""
    
    def compare(self, c_result: ExecutionResult, cpp_result: ExecutionResult, 
                strict: bool = True) -> ComparisonResult:
        """比较两个执行结果"""
        
        # 检查 stdout
        stdout_match = self._compare_output(
            c_result.stdout, cpp_result.stdout, strict
        )
        
        # 检查 stderr（可能包含调试信息，使用宽松模式）
        stderr_match = True  # stderr 可能不同（调试输出等）
        
        # 检查 exit code
        exit_code_match = (c_result.exit_code == cpp_result.exit_code)
        
        # 生成 diff
        diff_stdout = self._generate_diff(c_result.stdout, cpp_result.stdout)
        diff_stderr = self._generate_diff(c_result.stderr, cpp_result.stderr)
        
        passed = stdout_match and exit_code_match
        
        return ComparisonResult(
            passed=passed,
            stdout_match=stdout_match,
            stderr_match=stderr_match,
            exit_code_match=exit_code_match,
            diff_stdout=diff_stdout,
            diff_stderr=diff_stderr
        )
    
    def _compare_output(self, output1: str, output2: str, strict: bool) -> bool:
        """比较输出"""
        if strict:
            # 严格模式：精确匹配（忽略空白差异）
            lines1 = [l.strip() for l in output1.strip().split('\n') if l.strip()]
            lines2 = [l.strip() for l in output2.strip().split('\n') if l.strip()]
            return lines1 == lines2
        else:
            # 宽松模式：排序后比较
            lines1 = sorted(l.strip() for l in output1.strip().split('\n') if l.strip())
            lines2 = sorted(l.strip() for l in output2.strip().split('\n') if l.strip())
            return lines1 == lines2
    
    def _generate_diff(self, expected: str, actual: str) -> str:
        """生成差异报告"""
        import difflib
        
        expected_lines = expected.splitlines(keepends=True)
        actual_lines = actual.splitlines(keepends=True)
        
        diff = difflib.unified_diff(
            expected_lines, actual_lines,
            fromfile='expected (C)', tofile='actual (C++)',
            lineterm=''
        )
        
        return ''.join(diff)


# =============================================================================
# 3.4 Invariant Validator（不变量验证器）
# =============================================================================

class InvariantValidator:
    """不变量验证器 - 验证输出是否满足算法不变量"""
    
    def validate(self, test_case: TestCase, result: ExecutionResult) -> List[InvariantResult]:
        """验证不变量"""
        results = []
        
        # 1. 输出格式验证
        results.append(self._check_output_format(result))
        
        # 2. 校验和格式验证
        results.append(self._check_checksum_format(result))
        
        # 3. 无崩溃验证
        results.append(self._check_no_crash(result))
        
        # 4. 无内存错误验证
        results.append(self._check_no_memory_errors(result))
        
        return results
    
    def _check_output_format(self, result: ExecutionResult) -> InvariantResult:
        """检查输出格式"""
        if not result.stdout.strip():
            return InvariantResult(
                passed=True,
                name="output_format",
                message="Empty output (valid for some queries)"
            )
        
        # 检查每行是否为有效的校验和格式
        for line in result.stdout.strip().split('\n'):
            line = line.strip()
            if not line:
                continue
            
            # 校验和应该是数字
            try:
                int(line)
            except ValueError:
                return InvariantResult(
                    passed=False,
                    name="output_format",
                    message=f"Invalid checksum format: {line}"
                )
        
        return InvariantResult(
            passed=True,
            name="output_format",
            message="Valid checksum format"
        )
    
    def _check_checksum_format(self, result: ExecutionResult) -> InvariantResult:
        """检查校验和格式"""
        if result.exit_code != 0 and "ERROR" in result.stderr:
            return InvariantResult(
                passed=False,
                name="checksum_format",
                message=f"Program returned error: {result.stderr}"
            )
        
        return InvariantResult(
            passed=True,
            name="checksum_format",
            message="Checksum format OK"
        )
    
    def _check_no_crash(self, result: ExecutionResult) -> InvariantResult:
        """检查无崩溃"""
        if result.crash:
            return InvariantResult(
                passed=False,
                name="no_crash",
                message="Program crashed"
            )
        
        # 在 Unix 系统中，exit_code > 128 表示被信号终止
        # 例如 139 = 128 + 11 (SIGSEGV)
        if result.exit_code > 128:
            signal_num = result.exit_code - 128
            return InvariantResult(
                passed=False,
                name="no_crash",
                message=f"Program terminated with signal {signal_num}"
            )
        
        return InvariantResult(
            passed=True,
            name="no_crash",
            message="No crash detected"
        )
    
    def _check_no_memory_errors(self, result: ExecutionResult) -> InvariantResult:
        """检查无内存错误"""
        memory_errors = ["segmentation fault", "bus error", "stack overflow", 
                         "heap corruption", "double free", "invalid free"]
        
        stderr_lower = result.stderr.lower()
        for error in memory_errors:
            if error in stderr_lower:
                return InvariantResult(
                    passed=False,
                    name="no_memory_errors",
                    message=f"Memory error detected: {error}"
                )
        
        return InvariantResult(
            passed=True,
            name="no_memory_errors",
            message="No memory errors detected"
        )


# =============================================================================
# 3.5 Perf Analyzer（性能分析器）
# =============================================================================

class PerfAnalyzer:
    """性能分析器 - 比较 C 和 C++ 执行时间"""
    
    def __init__(self, config: Config):
        self.config = config
    
    def analyze(self, test_case: TestCase, executor: Executor) -> PerformanceResult:
        """分析性能"""
        c_times = []
        cpp_times = []
        
        # 多次运行取平均
        for _ in range(self.config.perf_iterations):
            c_result = executor.run(self.config.c_binary, test_case)
            cpp_result = executor.run(self.config.cpp_binary, test_case)
            
            if not c_result.timeout and not c_result.crash:
                c_times.append(c_result.duration_ms)
            if not cpp_result.timeout and not cpp_result.crash:
                cpp_times.append(cpp_result.duration_ms)
        
        if not c_times or not cpp_times:
            return PerformanceResult(
                c_time_ms=0,
                cpp_time_ms=0,
                ratio=0,
                verdict="SKIP"
            )
        
        c_avg = statistics.mean(c_times)
        cpp_avg = statistics.mean(cpp_times)
        
        if c_avg > 0:
            ratio = cpp_avg / c_avg
        else:
            ratio = 1.0
        
        # 判定
        if ratio < self.config.perf_threshold_warn:
            verdict = "OK"
        elif ratio < self.config.perf_threshold_fail:
            verdict = "WARN"
        else:
            verdict = "FAIL"
        
        return PerformanceResult(
            c_time_ms=c_avg,
            cpp_time_ms=cpp_avg,
            ratio=ratio,
            verdict=verdict
        )


# =============================================================================
# 3.7 Report Engine（报告系统）
# =============================================================================

class ReportEngine:
    """报告系统 - 生成验证报告"""
    
    def __init__(self, config: Config):
        self.config = config
        self.reports_dir = Path(config.reports_dir)
        self.reports_dir.mkdir(parents=True, exist_ok=True)
    
    def generate(self, report: VerificationReport) -> str:
        """生成报告"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = self.reports_dir / f"verification_report_{timestamp}.md"
        
        # 生成 Markdown 报告
        content = self._format_report(report)
        report_file.write_text(content)
        
        # 同时生成 JSON 报告
        json_file = self.reports_dir / f"verification_report_{timestamp}.json"
        json_file.write_text(json.dumps(asdict(report), indent=2, ensure_ascii=False))
        
        return str(report_file)
    
    def _format_report(self, report: VerificationReport) -> str:
        """格式化报告"""
        lines = [
            "# Verification Report",
            "",
            f"**Timestamp**: {report.timestamp}",
            "",
            "## Summary",
            "",
            f"- **Total Cases**: {report.total_cases}",
            f"- **Passed**: {report.passed}",
            f"- **Failed**: {report.failed}",
            f"- **Crashes (C)**: {report.crashes_c}",
            f"- **Crashes (C++)**: {report.crashes_cpp}",
            f"- **Timeouts**: {report.timeouts}",
            f"- **Invariant Failures**: {report.invariant_failures}",
            f"- **Performance Issues**: {report.performance_issues}",
            "",
            "## Coverage",
            "",
            f"- **Line Coverage**: {report.coverage_line:.1%}",
            f"- **Branch Coverage**: {report.coverage_branch:.1%}",
            "",
            "## Verdict",
            "",
            f"### {report.verdict}",
            "",
        ]
        
        if report.failures:
            lines.extend([
                "## Failures",
                "",
            ])
            
            for failure in report.failures[:20]:  # 只显示前20个失败
                lines.extend([
                    f"### Case: {failure['id']}",
                    "",
                    f"- **Category**: {failure['category']}",
                    f"- **Description**: {failure['description']}",
                    "",
                    "**Diff (stdout)**:",
                    "```diff",
                    failure.get('diff_stdout', 'N/A'),
                    "```",
                    "",
                ])
        
        if report.performance_results:
            lines.extend([
                "## Performance Analysis",
                "",
                "| Case | C (ms) | C++ (ms) | Ratio | Verdict |",
                "|------|--------|----------|-------|---------|",
            ])
            
            for perf in report.performance_results[:20]:
                lines.append(
                    f"| {perf['id']} | {perf['c_time_ms']:.2f} | "
                    f"{perf['cpp_time_ms']:.2f} | {perf['ratio']:.2f} | {perf['verdict']} |"
                )
        
        return "\n".join(lines)


# =============================================================================
# 主验证流程
# =============================================================================

class VerificationFramework:
    """验证框架主类"""
    
    def __init__(self, config: Config):
        self.config = config
        self.input_generator = InputGenerator(config)
        self.executor = Executor(config)
        self.comparator = Comparator()
        self.invariant_validator = InvariantValidator()
        self.perf_analyzer = PerfAnalyzer(config)
        self.report_engine = ReportEngine(config)
    
    def run(self) -> VerificationReport:
        """运行完整验证流程"""
        print("=" * 60)
        print("C → C++ 重构验证框架")
        print("=" * 60)
        
        report = VerificationReport(
            timestamp=datetime.now().isoformat()
        )
        
        # Step 1: 生成测试用例
        print("\n[Step 1] 生成测试用例...")
        test_cases = self.input_generator.generate_all()
        report.total_cases = len(test_cases)
        print(f"  生成 {len(test_cases)} 个测试用例")
        
        # Step 2: 执行对拍
        print("\n[Step 2] 执行对拍...")
        for i, case in enumerate(test_cases):
            if (i + 1) % 10 == 0:
                print(f"  进度: {i + 1}/{len(test_cases)}")
            
            # 执行 C 版本
            c_result = self.executor.run(self.config.c_binary, case)
            
            # 执行 C++ 版本
            cpp_result = self.executor.run(self.config.cpp_binary, case)
            
            # 统计超时
            if c_result.timeout or cpp_result.timeout:
                report.timeouts += 1
            
            # 比较
            comparison = self.comparator.compare(c_result, cpp_result)
            
            if comparison.passed:
                report.passed += 1
            else:
                report.failed += 1
                report.failures.append({
                    'id': case.id,
                    'name': case.name,
                    'category': case.category,
                    'description': case.description,
                    'diff_stdout': comparison.diff_stdout,
                    'diff_stderr': comparison.diff_stderr
                })
            
            # 不变量验证
            c_invariants = self.invariant_validator.validate(case, c_result)
            cpp_invariants = self.invariant_validator.validate(case, cpp_result)
            
            # 统计崩溃（基于不变量验证结果）
            for inv in c_invariants:
                if inv.name == "no_crash" and not inv.passed:
                    report.crashes_c += 1
                    break
            
            for inv in cpp_invariants:
                if inv.name == "no_crash" and not inv.passed:
                    report.crashes_cpp += 1
                    break
                if not inv.passed:
                    report.invariant_failures += 1
        
        # Step 3: 性能分析（仅对通过的用例）
        print("\n[Step 3] 性能分析...")
        for case in test_cases[:10]:  # 只分析前10个
            perf = self.perf_analyzer.analyze(case, self.executor)
            report.performance_results.append({
                'id': case.id,
                'c_time_ms': perf.c_time_ms,
                'cpp_time_ms': perf.cpp_time_ms,
                'ratio': perf.ratio,
                'verdict': perf.verdict
            })
            if perf.verdict == "FAIL":
                report.performance_issues += 1
        
        # Step 4: 生成判定
        print("\n[Step 4] 生成判定...")
        report.verdict = self._determine_verdict(report)
        
        # Step 5: 生成报告
        print("\n[Step 5] 生成报告...")
        report_file = self.report_engine.generate(report)
        print(f"  报告已保存: {report_file}")
        
        return report
    
    def _determine_verdict(self, report: VerificationReport) -> str:
        """判定最终结果"""
        # 检查是否有严重失败
        if report.failed > 0:
            return "❌ UNSAFE - 行为不一致"
        
        if report.crashes_cpp > 0:
            return "❌ UNSAFE - C++ 版本存在崩溃"
        
        if report.invariant_failures > 0:
            return "❌ UNSAFE - 不变量验证失败"
        
        if report.performance_issues > 0:
            return "⚠️ RISKY - 存在性能问题"
        
        if report.coverage_line < self.config.coverage_threshold:
            return "⚠️ RISKY - 覆盖率不足"
        
        return "✅ SAFE - 验证通过"


# =============================================================================
# 主入口
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='C → C++ 重构验证框架')
    parser.add_argument('--c-binary', default='../final/Driver', help='C 版本二进制路径')
    parser.add_argument('--cpp-binary', default='./build/linux/x86_64/debug/Driver', help='C++ 版本二进制路径')
    parser.add_argument('--workloads', default='./workloads/small', help='Workload 目录')
    parser.add_argument('--fuzz-count', type=int, default=100, help='模糊测试数量')
    parser.add_argument('--fuzz-seed', type=int, default=42, help='模糊测试种子')
    parser.add_argument('--timeout', type=int, default=10, help='超时时间（秒）')
    
    args = parser.parse_args()
    
    config = Config(
        c_binary=args.c_binary,
        cpp_binary=args.cpp_binary,
        workloads_dir=args.workloads,
        fuzz_count=args.fuzz_count,
        fuzz_seed=args.fuzz_seed,
        timeout_seconds=args.timeout
    )
    
    framework = VerificationFramework(config)
    report = framework.run()
    
    print("\n" + "=" * 60)
    print(f"最终判定: {report.verdict}")
    print("=" * 60)
    
    # 返回退出码
    if "UNSAFE" in report.verdict:
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
