#pragma once

#include <cstdint>

namespace radix_join {

// 前向声明
class Vector;
class Joiner;
class QueryInfo;
struct SelectInfo;

// 前向声明 - 用于避免循环引用
struct FilterArg;

/**
 * @brief 列信息结构体
 * 存储分区前后的列数据，包括值、行ID和元组
 */
struct ColumnInfo {
    uint64_t* values = nullptr;    ///< 列值数组
    Vector* tuples = nullptr;      ///< 指向 Vector 的指针（用于存储元组数据）
    unsigned* rowIds = nullptr;    ///< 行ID数组
};

/**
 * @brief 索引结构体
 * 用于构建阶段的哈希索引，每个 bucket 对应一个 Index
 * 
 * 使用链式哈希表解决冲突：
 * - bucketArray: 存储每个哈希值的链表头位置（1-based，0表示空）
 * - chainArray: 存储冲突链的下一个位置
 */
struct Index {
    unsigned* chainArray = nullptr;   ///< 链数组，存储相同哈希值的下一个位置
    unsigned* bucketArray = nullptr;  ///< 桶数组，存储每个哈希值的链表头
};

/**
 * @brief Radix Hash Join 信息结构体
 * 
 * 该结构体保存了执行 Radix Hash Join 所需的所有信息，包括：
 * - 关系/列标识信息
 * - 原始列数据和分区后的数据
 * - 直方图和前缀和（用于分区定位）
 * - 哈希索引数组（用于探测阶段）
 * 
 * 数据流：
 * 1. partition阶段: col/unsorted -> sorted (基于HASH_FUN_1的第一级分区)
 * 2. build阶段: 在sorted上为small关系构建索引 (基于HASH_FUN_2的第二级哈希)
 * 3. probe阶段: 遍历big关系，在small的索引中查找匹配
 */
struct RadixHashJoinInfo {
    // 位置和标识信息
    unsigned pos = 0;          ///< 在intermediate结果中的位置
    unsigned relId = 0;        ///< 关系ID（相对于解析顺序）
    unsigned colId = 0;        ///< 列ID
    
    // 原始列数据（当isInInter=0时使用）
    uint64_t* col = nullptr;   ///< 指向原始列值的指针
    
    // 元组信息
    unsigned numOfTuples = 0;  ///< 元组数量（原始关系或中间结果中）
    unsigned tupleSize = 0;    ///< 每个元组中的rowId数量（仅在isInInter=1时使用）
    
    // 中间结果向量（当isInInter=1时使用）
    Vector** vector = nullptr;     ///< 指向中间结果向量的指针
    Vector** ptrToVec = nullptr;   ///< vector在intermediate中的地址（用于清理）
    
    // 映射数组（当isInInter=1时使用）
    unsigned* map = nullptr;       ///< 关系ID到元组位置的映射
    unsigned** ptrToMap = nullptr; ///< map的地址（用于清理）
    
    // 分区相关数据
    ColumnInfo* unsorted = nullptr;  ///< 分区前的数据
    ColumnInfo* sorted = nullptr;    ///< 分区后的数据
    unsigned* hist = nullptr;        ///< 直方图：每个bucket的元素数量
    unsigned* pSum = nullptr;        ///< 前缀和：每个bucket在sorted中的起始位置
    
    // 哈希索引数组（build阶段构建）
    Index** indexArray = nullptr;    ///< HASH_RANGE_1个bucket的索引数组
    
    // 标志位
    unsigned isInInter = 0;   ///< 1表示在中间结果中，0表示不在
    unsigned isSmall = 0;     ///< 1表示是较小的关系（用于构建索引）
    unsigned isLeft = 0;      ///< 1表示在join的左侧
    
    // 查询关系数量
    unsigned queryRelations = 0;  ///< 查询中参与的关系数量
};

/**
 * @brief 中间结果元数据结构体
 * 
 * 该结构体管理查询执行过程中的所有中间结果，包括：
 * - 中间结果向量数组（每个向量数组对应一个中间实体）
 * - 关系映射数组（记录每个关系在元组中的位置）
 * 
 * 数据流说明：
 * - interResults: 三维数组 [maxNumOfVectors][HASH_RANGE_1]，存储中间结果的 Vector 指针
 * - mapRels: 二维数组 [maxNumOfVectors][queryRelations]，记录关系ID到元组位置的映射
 *   例如：mapRels[0][1] = 2 表示第0个向量的元组中，关系1的rowId位于位置2
 */
struct InterMetaData {
    /**
     * 三维向量数组：每个中间实体对应一个 Vector 指针数组
     * 使用多线程并行计算中间结果，每个线程处理一个分区
     */
    Vector*** interResults = nullptr;

    /**
     * 二维映射数组：每个中间实体对应一个映射数组
     * mapRels[i][relId] = tupleOffset 表示关系relId的rowId在元组中的偏移位置
     * -1 表示该关系不在此中间实体中
     */
    unsigned** mapRels = nullptr;

    /* 查询中参与的关系数量 */
    unsigned queryRelations = 0;

    /* interResults 数组的大小，即可能创建的最大中间实体数量 */
    unsigned maxNumOfVectors = 0;
};

/*==============================================================================
 * 创建器/初始化器
 *============================================================================*/

/**
 * @brief 创建中间结果元数据
 * @param inter 输出指针的引用
 * @param q 查询信息
 */
void createInterMetaData(InterMetaData** inter, QueryInfo* q);

/**
 * @brief 初始化 RadixHashJoinInfo 结构
 * @param inter 中间结果元数据
 * @param q 查询信息
 * @param s 选择信息
 * @param j Joiner 对象
 * @param arg 输出的 RadixHashJoinInfo 结构
 */
void initializeInfo(InterMetaData* inter, QueryInfo* q, SelectInfo* s, Joiner* j, RadixHashJoinInfo* arg);

/*==============================================================================
 * 应用函数 - 查询执行主流程
 *============================================================================*/

/**
 * @brief 应用列等值谓词（同一关系内的列比较，如 r1.a = r1.b）
 * @param inter 中间结果元数据
 * @param joiner Joiner 对象
 * @param q 查询信息
 */
void applyColumnEqualities(InterMetaData* inter, Joiner* joiner, QueryInfo* q);

/**
 * @brief 应用过滤条件
 * @param inter 中间结果元数据
 * @param joiner Joiner 对象
 * @param q 查询信息
 */
void applyFilters(InterMetaData* inter, Joiner* joiner, QueryInfo* q);

/**
 * @brief 应用连接操作
 * @param inter 中间结果元数据
 * @param joiner Joiner 对象
 * @param q 查询信息
 */
void applyJoins(InterMetaData* inter, Joiner* joiner, QueryInfo* q);

/**
 * @brief 执行实际的连接操作（根据左右关系的位置选择策略）
 * @param inter 中间结果元数据
 * @param argLeft 左关系信息
 * @param argRight 右关系信息
 */
void applyProperJoin(InterMetaData* inter, RadixHashJoinInfo* argLeft, RadixHashJoinInfo* argRight);

/**
 * @brief 计算并输出校验和
 * @param inter 中间结果元数据
 * @param joiner Joiner 对象
 * @param q 查询信息
 */
void applyCheckSums(InterMetaData* inter, Joiner* joiner, QueryInfo* q);

/*==============================================================================
 * 四种 Join 策略
 *============================================================================*/

/**
 * @brief 非中间结果与非中间结果的连接
 * 两个关系都不在中间结果中（首次连接）
 */
void joinNonInterNonInter(InterMetaData* inter, RadixHashJoinInfo* left, RadixHashJoinInfo* right);

/**
 * @brief 非中间结果与中间结果的连接
 * 左关系不在中间结果中，右关系在中间结果中
 */
void joinNonInterInter(InterMetaData* inter, RadixHashJoinInfo* left, RadixHashJoinInfo* right);

/**
 * @brief 中间结果与非中间结果的连接
 * 左关系在中间结果中，右关系不在中间结果中
 */
void joinInterNonInter(InterMetaData* inter, RadixHashJoinInfo* left, RadixHashJoinInfo* right);

/**
 * @brief 中间结果与中间结果的连接
 * 两个关系都在中间结果中
 */
void joinInterInter(InterMetaData* inter, RadixHashJoinInfo* left, RadixHashJoinInfo* right);

/*==============================================================================
 * 检查函数
 *============================================================================*/

/**
 * @brief 检查向量是否在中间结果中
 * @param vector 向量指针
 * @return 1 如果在中间结果中，0 否则
 */
unsigned isInInter(Vector* vector);

/*==============================================================================
 * 辅助函数
 *============================================================================*/

/**
 * @brief 获取关系所在的向量位置
 * @param inter 中间结果元数据
 * @param relId 关系ID
 * @return 向量位置索引
 */
unsigned getVectorPos(InterMetaData* inter, unsigned relId);

/**
 * @brief 获取第一个可用的向量位置
 * @param inter 中间结果元数据
 * @return 第一个可用位置的索引
 */
unsigned getFirstAvailablePos(InterMetaData* inter);

/**
 * @brief 创建映射数组
 * @param mapRels 输出映射数组的指针
 * @param size 数组大小
 * @param values 初始值数组
 */
void createMap(unsigned** mapRels, unsigned size, unsigned* values);

/**
 * @brief 打印校验和
 * @param checkSum 校验和值
 * @param isLast 是否为最后一个校验和
 */
void printCheckSum(uint64_t checkSum, unsigned isLast);

/*==============================================================================
 * 销毁器
 *============================================================================*/

/**
 * @brief 销毁中间结果元数据
 * @param inter 中间结果元数据指针
 */
void destroyInterMetaData(InterMetaData* inter);

/**
 * @brief 销毁 RadixHashJoinInfo 结构
 * @param info RadixHashJoinInfo 指针
 */
void destroyRadixHashJoinInfo(RadixHashJoinInfo* info);

/**
 * @brief 销毁 ColumnInfo 结构
 * @param c ColumnInfo 指针的指针
 */
void destroyColumnInfo(ColumnInfo** c);

} // namespace radix_join
