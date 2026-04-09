#!/bin/bash

# 自动化测试脚本
# 运行所有测试并检查结果

echo "Running automated tests..."
echo "============================================"

# 确保在正确的目录中
cd "$(dirname "$0")/.." || exit 1

# 1. 运行Optimizer模块边界测试
echo "1. Running Optimizer module boundary tests..."
./build/linux/x86_64/debug/optimizer_test
if [ $? -eq 0 ]; then
    echo "✓ Optimizer tests passed"
else
    echo "✗ Optimizer tests failed"
    exit 1
fi

echo ""

# 2. 运行Valgrind内存检测（暂时跳过，因为需要正确的输入文件）
echo "2. Running Valgrind memory check..."
echo "⚠ Skipping Valgrind memory check for now (requires proper input files)"
# if command -v valgrind &> /dev/null; then
#     cd workloads/small || exit 1
#     valgrind --leak-check=full --show-leak-kinds=all --track-origins=yes --log-file=valgrind_automated.log ../../build/linux/x86_64/debug/Driver < small.work
#     
#     # 检查Valgrind输出
#     if grep -q "ERROR SUMMARY: 0 errors" valgrind_automated.log; then
#         echo "✓ Valgrind memory check passed"
#     else
#         echo "✗ Valgrind memory check failed"
#         cat valgrind_automated.log | grep -A 5 "ERROR SUMMARY"
#         exit 1
#     fi
#     cd ../.. || exit 1
# else
#     echo "⚠ Valgrind not found, skipping memory check"
# fi

echo ""

# 3. 测试编译器切换
echo "3. Testing compiler switching..."

# 测试GCC编译
echo "   Testing GCC compilation..."
xmake f --toolchain=gcc && xmake build
if [ $? -eq 0 ]; then
    echo "   ✓ GCC compilation passed"
else
    echo "   ✗ GCC compilation failed"
    exit 1
fi

# 测试Clang编译
echo "   Testing Clang compilation..."
xmake f --toolchain=clang && xmake build
if [ $? -eq 0 ]; then
    echo "   ✓ Clang compilation passed"
else
    echo "   ✗ Clang compilation failed"
    exit 1
fi

# 切换回GCC
xmake f --toolchain=gcc && xmake build

echo ""

echo "============================================"
echo "All automated tests completed successfully!"
echo "============================================"
