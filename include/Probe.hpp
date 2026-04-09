#pragma once

#include "Vector.hpp"
#include "Partition.hpp"

namespace radix_join {

/**
 * @brief Join 任务参数
 * 
 * 每个线程负责探测一个 bucket
 */
struct JoinArg {
    unsigned bucket = 0;               ///< bucket 编号
    RadixHashJoinInfo* left = nullptr;  ///< 左关系信息
    RadixHashJoinInfo* right = nullptr; ///< 右关系信息
    Vector* results = nullptr;          ///< 结果向量
};

/**
 * @brief Join 线程函数
 * 
 * 探测指定的 bucket，查找匹配项并插入结果向量
 * 
 * @param arg 指向 JoinArg 的指针
 */
void joinFunc(void* arg);

/**
 * @brief 检查相等性并构造结果元组
 * 
 * 检查 small 关系和 big 关系在指定位置的值是否相等，
 * 如果相等则构造结果元组并插入结果向量。
 * 
 * @param small 较小的关系（已构建索引）
 * @param big 较大的关系（未构建索引）
 * @param i big 关系的当前行
 * @param start small 关系 bucket 的起始位置
 * @param searchValue 要搜索的值
 * @param pseudoRow bucket 内的相对行号（用于索引查找）
 * @param results 结果向量
 * @param tupleToInsert 用于存储构造的元组
 */
void checkEqual(RadixHashJoinInfo* small, RadixHashJoinInfo* big, 
                unsigned i, unsigned start, unsigned searchValue, 
                unsigned pseudoRow, Vector* results, unsigned* tupleToInsert);

/**
 * @brief 构造结果元组
 * 
 * 根据 small 和 big 关系的匹配行构造结果元组。
 * 元组格式：[left tuple][right tuple]
 * 
 * @param small 较小的关系
 * @param big 较大的关系
 * @param actualRow small 关系的实际行号
 * @param i big 关系的当前行
 * @param target 输出元组缓冲区
 */
void constructTuple(RadixHashJoinInfo* small, RadixHashJoinInfo* big, 
                    unsigned actualRow, unsigned i, unsigned* target);

/**
 * @brief 探测阶段主函数
 * 
 * 这是 Radix Hash Join 的第三阶段：探测
 * 
 * 算法步骤：
 * 1. 确定 small 和 big 关系（根据 isSmall 标志）
 * 2. 为每个 bucket 提交探测任务
 * 3. 等待所有任务完成
 * 
 * 每个线程的工作：
 * - 遍历 big 关系中对应 bucket 的所有元组
 * - 对每个元组，在 small 关系的索引中查找匹配
 * - 如果找到匹配，构造结果元组并插入结果向量
 * 
 * @param left 左关系信息
 * @param right 右关系信息
 * @param results 结果向量
 */
void probe(RadixHashJoinInfo* left, RadixHashJoinInfo* right, Vector* results);

} // namespace radix_join
