#pragma once

#include <cstdint>
#include <climits>  // for CHAR_BIT
#include <limits>

#include "Parser.hpp"
#include "Relation.hpp"

namespace radix_join {

// 前向声明
class Joiner;

// 素数上限常量 - 用于位图大小限制
inline constexpr unsigned PRIMELIMIT = 49999991;

/**
 * @brief 位图操作内联函数
 * 将C宏转换为C++ constexpr inline函数
 */

/**
 * @brief 计算位掩码
 * @param b 位位置
 * @return 对应位的掩码值
 */
[[nodiscard]] constexpr char BITMASK(unsigned b) noexcept {
    return static_cast<char>(1 << (b % CHAR_BIT));
}

/**
 * @brief 计算位槽索引
 * @param b 位位置
 * @return 包含该位的字节索引
 */
[[nodiscard]] constexpr unsigned BITSLOT(unsigned b) noexcept {
    return b / CHAR_BIT;
}

/**
 * @brief 设置位向量中的某一位
 * @param v 位向量（char数组）
 * @param b 要设置的位位置
 */
inline void BITSET(char* v, unsigned b) noexcept {
    v[BITSLOT(b)] |= BITMASK(b);
}

/**
 * @brief 清除位向量中的某一位
 * @param v 位向量（char数组）
 * @param b 要清除的位位置
 */
inline void BITCLEAR(char* v, unsigned b) noexcept {
    v[BITSLOT(b)] &= ~BITMASK(b);
}

/**
 * @brief 测试位向量中的某一位
 * @param v 位向量（char数组）
 * @param b 要测试的位位置
 * @return 非零如果该位被设置，0否则
 */
[[nodiscard]] inline char BITTEST(const char* v, unsigned b) noexcept {
    return v[BITSLOT(b)] & BITMASK(b);
}

/**
 * @brief 计算位向量所需的槽位数
 * @param nb 位的总数
 * @return 所需的字节数
 */
[[nodiscard]] constexpr unsigned BITNSLOTS(unsigned nb) noexcept {
    return (nb + CHAR_BIT - 1) / CHAR_BIT;
}

/**
 * @brief 计算列统计信息
 * 分析列数据，计算最小值、最大值、不同值数量等统计信息
 * 
 * @param column 列数据指针
 * @param stat 输出统计信息结构体
 * @param columnSize 列中元素数量
 */
void findStats(const uint64_t* column, ColumnStats* stat, unsigned columnSize);

/**
 * @brief 应用列等值估计
 * 处理同一关系内不同列之间的等值谓词（如 r1.a = r1.b）
 * 更新查询的统计信息估计
 * 
 * @param q 查询信息
 * @param j Joiner对象
 */
void applyColEqualityEstimations(QueryInfo& q, const Joiner& j);

/**
 * @brief 应用过滤器估计
 * 处理过滤条件（如 r1.a > 100），更新统计信息
 * 
 * @param j Joiner对象
 * @param q 查询信息
 * @param colId 列ID
 * @param stat 统计信息结构体
 * @param actualRelId 实际关系ID
 * @param relId 查询中的关系索引
 * @param cmp 比较操作符
 * @param constant 常量值
 */
void filterEstimation(const Joiner& j, QueryInfo& q, unsigned colId, ColumnStats* stat,
                      unsigned actualRelId, unsigned relId, Comparison cmp, uint64_t constant);

/**
 * @brief 应用所有过滤器估计
 * 遍历查询中的所有过滤器并应用统计估计
 * 
 * @param q 查询信息
 * @param j Joiner对象
 */
void applyFilterEstimations(QueryInfo& q, const Joiner& j);

/**
 * @brief 应用连接估计
 * 处理不同关系之间的连接谓词，更新统计信息
 * 
 * @param q 查询信息
 * @param j Joiner对象
 */
void applyJoinEstimations(QueryInfo& q, const Joiner& j);

/**
 * @brief 查找最优连接顺序
 * 基于统计信息计算最优的连接执行顺序
 * 
 * @param q 查询信息
 * @param j Joiner对象
 */
void findOptimalJoinOrder(QueryInfo& q, const Joiner& j);

/* 打印函数 - 用于调试 */

/**
 * @brief 打印列内容
 * @param column 列数据指针
 * @param columnSize 列大小
 */
void columnPrint(const uint64_t* column, unsigned columnSize);

/**
 * @brief 打印布尔数组
 * @param array 布尔数组
 * @param size 数组大小
 */
void printBooleanArray(const char* array, unsigned size);

/**
 * @brief 打印列统计信息
 * @param s 统计信息结构体
 */
void printColumnStats(const ColumnStats* s);

} // namespace radix_join
