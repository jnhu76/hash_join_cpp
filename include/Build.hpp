#pragma once

#include "Partition.hpp"

namespace radix_join {

// 第二级哈希范围（用于构建索引）
inline constexpr unsigned HASH_RANGE_2 = 301;

/**
 * @brief 第二级哈希函数
 * 
 * 用于在单个 bucket 内构建哈希索引
 * 使用取模运算将值映射到 HASH_RANGE_2 个位置
 * 
 * @param key 输入键值
 * @return 哈希值 (0 ~ HASH_RANGE_2-1)
 */
constexpr inline unsigned HASH_FUN_2(uint64_t key) noexcept {
    return static_cast<unsigned>(key % HASH_RANGE_2);
}

/**
 * @brief 构建索引任务参数
 * 
 * 每个线程负责为一个 bucket 构建索引
 */
struct BuildArg {
    unsigned bucket = 0;              ///< bucket 编号
    RadixHashJoinInfo* info = nullptr; ///< 构建信息
};

/**
 * @brief 构建索引线程函数
 * 
 * 为指定的 bucket 构建哈希索引
 * 使用链式哈希表解决冲突
 * 
 * @param arg 指向 BuildArg 的指针
 */
void buildFunc(void* arg);

/**
 * @brief 构建阶段主函数
 * 
 * 确定哪个关系较小，并为较小的关系构建索引
 * 
 * 算法步骤：
 * 1. 比较两个关系的元组数量，确定 small 和 big
 * 2. 为 small 关系初始化索引数组
 * 3. 为每个 bucket 提交构建任务
 * 4. 等待所有任务完成
 * 
 * @param infoLeft 左关系信息
 * @param infoRight 右关系信息
 */
void build(RadixHashJoinInfo* infoLeft, RadixHashJoinInfo* infoRight);

/**
 * @brief 初始化索引数组
 * 
 * 为每个非空 bucket 分配索引结构
 * 包括 chainArray 和 bucketArray 的内存分配
 * 
 * @param info 需要构建索引的关系信息
 */
void initializeIndexArray(RadixHashJoinInfo* info);

/**
 * @brief 遍历冲突链
 * 
 * 在 chainArray 中找到第一个空位，存储新位置
 * 用于处理哈希冲突
 * 
 * @param chainPos 当前链位置
 * @param chainArray 链数组
 * @param posToBeStored 要存储的位置值
 */
void traverseChain(unsigned chainPos, unsigned* chainArray, unsigned posToBeStored);

} // namespace radix_join
