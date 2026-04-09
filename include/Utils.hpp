#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>
#include <iostream>

namespace radix_join {

// 调试输出控制宏 - 定义 DEBUG_OUTPUT 以启用调试输出
// #define DEBUG_OUTPUT

#ifdef DEBUG_OUTPUT
    #define DEBUG_LOG(msg) std::cerr << msg << std::endl
#else
    #define DEBUG_LOG(msg) ((void)0)
#endif

// 缓冲区大小常量
inline constexpr std::size_t BUFFERSIZE = 512;

/**
 * @brief 比较操作枚举类
 * 用于表示数值比较操作：小于、大于、等于
 */
enum class Comparison {
    Less = '<',      // 小于
    Greater = '>',   // 大于
    Equal = '='      // 等于
};

/**
 * @brief 比较函数
 * 根据给定的比较操作符，比较key和constant
 * 
 * @param key 待比较的键值
 * @param cmp 比较操作符
 * @param constant 用于比较的常量值
 * @return true 如果比较条件满足
 * @return false 如果比较条件不满足
 */
[[nodiscard]] constexpr bool compare(uint64_t key, Comparison cmp, uint64_t constant) noexcept {
    switch (cmp) {
        case Comparison::Equal:
            return key == constant;
        case Comparison::Less:
            return key < constant;
        case Comparison::Greater:
            return key > constant;
    }
    return false;  // 不可达，但编译器需要
}

/**
 * @brief 快速幂运算（二分幂）
 * 使用递归二分法计算 base^exponent，时间复杂度 O(log n)
 * 
 * @param base 底数
 * @param exponent 指数
 * @return uint64_t 计算结果
 */
[[nodiscard]] constexpr uint64_t power(uint64_t base, uint64_t exponent) noexcept {
    if (exponent == 0) {
        return 1;
    } else if (exponent % 2 == 0) {
        uint64_t temp = power(base, exponent / 2);
        return temp * temp;
    } else {
        return base * power(base, exponent - 1);
    }
}

/**
 * @brief 线性幂运算
 * 使用循环计算 base^exponent，时间复杂度 O(n)
 * 适用于小指数场景
 * 
 * @param base 底数
 * @param exponent 指数
 * @return uint64_t 计算结果
 */
[[nodiscard]] constexpr uint64_t linearPower(uint64_t base, uint64_t exponent) noexcept {
    uint64_t result = 1;
    for (uint64_t i = 0; i < exponent; ++i) {
        result *= base;
    }
    return result;
}

/**
 * @brief 内存分配检查函数
 * 在C++中替代C版本的MALLOC_CHECK宏
 * 当指针为空时抛出异常
 * 
 * @param ptr 待检查的指针
 * @param file 源文件名
 * @param line 行号
 * @throws std::runtime_error 当内存分配失败时
 */
inline void mallocCheck(const void* ptr, const char* file, int line) {
    if (ptr == nullptr) {
        throw std::runtime_error(
            std::string("[ERROR] Memory allocation failed at ") + file + ":" + std::to_string(line)
        );
    }
}

} // namespace radix_join
