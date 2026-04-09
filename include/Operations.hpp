#pragma once

#include <cstdint>
#include "Utils.hpp"

namespace radix_join {

// 前向声明
class Vector;
class InterMetaData;
struct RadixHashJoinInfo;

/*==============================================================================
 * 过滤任务参数结构体
 *============================================================================*/

/**
 * @brief 过滤任务参数
 * 
 * 用于 filterFunc 线程函数，包含过滤操作所需的所有参数
 */
struct FilterArg {
    uint64_t* col = nullptr;       ///< 列数据指针
    uint64_t constant = 0;         ///< 比较常量
    Comparison cmp = Comparison::Equal;  ///< 比较操作符
    unsigned start = 0;            ///< 起始位置
    unsigned end = 0;              ///< 结束位置
    Vector** vector = nullptr;     ///< 输出向量指针
};

/*==============================================================================
 * 线程函数声明
 *============================================================================*/

/**
 * @brief 过滤线程函数
 * 
 * 工作线程执行的过滤任务，扫描指定范围的列数据，
 * 将满足条件的行ID插入到结果向量中。
 * 
 * @param arg 指向 FilterArg 的指针
 */
void filterFunc(void* arg);

/*==============================================================================
 * 列等值操作函数
 *============================================================================*/

/**
 * @brief 对中间结果应用列等值过滤
 * 
 * 当关系已在中间结果中时，扫描中间结果中的元组，
 * 保留满足 leftCol[rowIdLeft] == rightCol[rowIdRight] 的元组。
 * 
 * @param leftCol 左列数据
 * @param rightCol 右列数据
 * @param posLeft 左 rowId 在元组中的位置
 * @param posRight 右 rowId 在元组中的位置
 * @param vector 中间结果向量数组
 */
void colEqualityInter(uint64_t* leftCol, uint64_t* rightCol, unsigned posLeft, unsigned posRight, Vector** vector);

/**
 * @brief 对原始关系应用列等值过滤
 * 
 * 当关系不在中间结果中时，扫描原始关系的所有元组，
 * 保留满足 leftCol[i] == rightCol[i] 的行。
 * 
 * @param leftCol 左列数据
 * @param rightCol 右列数据
 * @param numOfTuples 元组数量
 * @param vector 输出向量数组
 */
void colEquality(uint64_t* leftCol, uint64_t* rightCol, unsigned numOfTuples, Vector** vector);

/*==============================================================================
 * 过滤操作函数
 *============================================================================*/

/**
 * @brief 对中间结果应用过滤条件
 * 
 * 扫描中间结果中的元组，保留满足 compare(col[rowId], cmp, constant) 的元组。
 * 
 * @param col 列数据
 * @param cmp 比较操作符
 * @param constant 比较常量
 * @param vector 中间结果向量数组（输入输出）
 */
void filterInter(uint64_t* col, Comparison cmp, uint64_t constant, Vector** vector);

/*==============================================================================
 * Join 操作函数 - 四种策略
 *============================================================================*/

/**
 * @brief 非中间结果与非中间结果的连接
 * 
 * 两个关系都不在中间结果中（首次连接）。
 * 执行完整的分区-构建-探测流程，创建新的中间结果。
 * 
 * @param inter 中间结果元数据
 * @param left 左关系信息
 * @param right 右关系信息
 */
void joinNonInterNonInter(InterMetaData* inter, RadixHashJoinInfo* left, RadixHashJoinInfo* right);

/**
 * @brief 非中间结果与中间结果的连接
 * 
 * 左关系不在中间结果中，右关系在中间结果中。
 * 将新关系与已有中间结果连接。
 * 
 * @param inter 中间结果元数据
 * @param left 左关系信息（新关系）
 * @param right 右关系信息（中间结果）
 */
void joinNonInterInter(InterMetaData* inter, RadixHashJoinInfo* left, RadixHashJoinInfo* right);

/**
 * @brief 中间结果与非中间结果的连接
 * 
 * 左关系在中间结果中，右关系不在中间结果中。
 * 将已有中间结果与新关系连接。
 * 
 * @param inter 中间结果元数据
 * @param left 左关系信息（中间结果）
 * @param right 右关系信息（新关系）
 */
void joinInterNonInter(InterMetaData* inter, RadixHashJoinInfo* left, RadixHashJoinInfo* right);

/**
 * @brief 中间结果与中间结果的连接
 * 
 * 两个关系都在中间结果中。
 * 如果两个关系在同一个中间结果向量中，使用列等值过滤；
 * 否则，执行完整的分区-构建-探测流程合并两个中间结果。
 * 
 * @param inter 中间结果元数据
 * @param left 左关系信息
 * @param right 右关系信息
 */
void joinInterInter(InterMetaData* inter, RadixHashJoinInfo* left, RadixHashJoinInfo* right);

} // namespace radix_join
