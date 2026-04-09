#!/bin/bash

# 定期内存检测脚本
# 使用Valgrind进行全面内存分析

echo "Running Valgrind memory analysis..."
echo "============================================"

# 确保在正确的目录中
cd "$(dirname "$0")/.." || exit 1

# 确保构建目录存在
if [ ! -d "build/linux/x86_64/debug" ]; then
    echo "Building project..."
    xmake build
    if [ $? -ne 0 ]; then
        echo "Build failed, exiting"
        exit 1
    fi
fi

# 运行Valgrind内存检测
echo "Running Valgrind with leak check..."

# 创建输出目录
mkdir -p valgrind_reports

# 生成时间戳
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
OUTPUT_FILE="valgrind_reports/valgrind_${TIMESTAMP}.log"

echo "Output will be saved to: ${OUTPUT_FILE}"

# 运行Valgrind（使用optimizer_test，不依赖外部文件）
valgrind --leak-check=full --show-leak-kinds=all --track-origins=yes --verbose --log-file="${OUTPUT_FILE}" build/linux/x86_64/debug/optimizer_test

# 检查Valgrind输出
if grep -q "ERROR SUMMARY: 0 errors" "${OUTPUT_FILE}"; then
    echo "✓ Valgrind memory check passed"
    echo "Memory analysis completed successfully!"
else
    echo "✗ Valgrind memory check failed"
    echo "Errors found in memory analysis."
    echo "Please check the report file: ${OUTPUT_FILE}"
    # 显示错误摘要
    cat "${OUTPUT_FILE}" | grep -A 10 "ERROR SUMMARY"
    exit 1
fi

# 清理旧的报告文件（保留最近10个）
echo "Cleaning up old reports..."
cd valgrind_reports || exit 1
ls -t valgrind_*.log | tail -n +11 | xargs rm -f 2>/dev/null
cd .. || exit 1

echo "============================================"
echo "Memory analysis completed successfully!"
echo "Report saved to: ${OUTPUT_FILE}"
echo "============================================"
