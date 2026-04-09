#pragma once

#include <cstdint>
#include "Intermediate.hpp"
#include "Joiner.hpp"

namespace radix_join {

// 使用Joiner.hpp中定义的HASH_FUN_1宏

// 并行分区开关（保留为宏以便编译时优化）
#define PARALLEL_PARTITION 1
#define PARALLEL_HISTOGRAM 1

// 负载均衡开关（保留为宏以便编译时优化）
#define ENABLE_LOAD_BALANCE 1

/**
 * @brief 直方图计算任务参数
 * 
 * 每个线程负责计算一段数据的直方图
 * 直方图统计每个哈希值出现的次数
 */
struct HistArg {
    unsigned start = 0;        ///< 起始位置
    unsigned end = 0;          ///< 结束位置
    uint64_t* values = nullptr; ///< 输入值数组
    unsigned* histogram = nullptr; ///< 输出直方图数组
};

/**
 * @brief 分区任务参数
 * 
 * 每个线程负责将一段数据根据哈希值分区到 sorted 数组中
 * 使用 pSumCopy 来确定每个元素在 sorted 中的位置
 */
struct PartitionArg {
    unsigned start = 0;           ///< 起始位置
    unsigned end = 0;             ///< 结束位置
    unsigned* pSumCopy = nullptr; ///< 前缀和副本（用于定位写入位置）
    RadixHashJoinInfo* info = nullptr; ///< 分区信息
};

/**
 * @brief 直方图计算线程函数
 * 
 * 计算指定数据范围的直方图，统计每个哈希值的出现次数
 * 计算完成后通过 barrier 同步
 * 
 * @param arg 指向 HistArg 的指针
 */
void histFunc(void* arg);

/**
 * @brief 分区线程函数
 * 
 * 将数据根据 HASH_FUN_1 分区到 sorted 数组中
 * 使用互斥锁保护 pSumCopy 的并发访问
 * 分区完成后通过 barrier 同步
 * 
 * @param arg 指向 PartitionArg 的指针
 */
void partitionFunc(void* arg);

/**
 * @brief 分区主函数
 * 
 * 执行 Radix Hash Join 的第一阶段：分区
 * 
 * 算法步骤：
 * 1. 分配 unsorted 和 sorted 的内存空间
 * 2. 如果数据在中间结果中，调用 scanJoin 获取数据
 * 3. 计算直方图（可并行）
 * 4. 计算前缀和 pSum
 * 5. 根据哈希值将数据分区到 sorted（可并行）
 * 
 * 分区后，相同哈希值的元素在 sorted 中是连续的
 * 
 * @param info RadixHashJoinInfo 指针，包含输入输出信息
 */
void partition(RadixHashJoinInfo* info);

} // namespace radix_join
