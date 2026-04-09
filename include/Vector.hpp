#pragma once

#include <cstdint>
#include <cstddef>
#include <iostream>
#include "Utils.hpp"

namespace radix_join {

// 全局初始大小，由 Joiner 模块根据平均元组数初始化
extern unsigned initSize;

/**
 * @brief Vector 类 - 动态数组实现，用于存储元组（tuple）
 * 
 * 该类实现了动态扩容的数组，专门用于存储行ID（rowId）。
 * 内部使用 unsigned* 数组（通过 new 分配），保持与原始C代码相同的内存布局，
 * 以便其他模块（如 Probe、Intermediate）可以直接通过指针访问 table[pos]。
 * 
 * 关键设计：
 * - table 使用 new unsigned[] 分配，保持 unsigned* 类型以兼容旧代码
 * - 支持动态扩容（容量不足时翻倍）
 * - 支持固定大小初始化（用于已知数据量场景）
 */
class Vector {
public:
    /**
     * @brief 默认构造函数
     * 创建一个空的 Vector，需要后续设置 tupleSize
     */
    Vector() = default;

    /**
     * @brief 构造函数
     * @param tupleSize 每个元组的大小（即每行包含的 rowId 数量）
     */
    explicit Vector(unsigned tupleSize);

    /**
     * @brief 固定大小构造函数
     * 用于预先知道需要存储多少元组的场景
     * 
     * @param tupleSize 每个元组的大小
     * @param fixedSize 需要存储的元组数量
     */
    Vector(unsigned tupleSize, unsigned fixedSize);

    /**
     * @brief 析构函数
     * 释放 table 占用的内存
     */
    ~Vector();

    // 禁止拷贝（原始C代码语义）
    Vector(const Vector&) = delete;
    Vector& operator=(const Vector&) = delete;

    // 允许移动语义
    Vector(Vector&& other) noexcept;
    Vector& operator=(Vector&& other) noexcept;

    /**
     * @brief 在 Vector 尾部插入一个元组
     * 如果容量不足，会自动扩容（翻倍）
     * 
     * @param tuple 指向要插入的元组数据的指针
     */
    void insert(const unsigned* tuple);

    /**
     * @brief 在指定位置插入一个元组
     * 仅用于固定大小 Vector，在特定偏移位置插入
     * 
     * @param tuple 指向要插入的元组数据的指针
     * @param offset 元组索引位置（不是字节偏移）
     */
    void insertAt(const unsigned* tuple, unsigned offset);

    /**
     * @brief 检查 Vector 是否已满
     * @return true 如果 nextPos == capacity
     */
    [[nodiscard]] bool isFull() const noexcept;

    /**
     * @brief 检查 Vector 是否为空（未分配 table）
     * @return true 如果 table == nullptr
     */
    [[nodiscard]] bool isEmpty() const noexcept;

    /**
     * @brief 获取当前存储的元组数量
     * @return 元组数量（nextPos / tupleSize）
     */
    [[nodiscard]] unsigned getTupleCount() const noexcept;

    /**
     * @brief 获取每个元组的大小
     * @return 元组大小（rowId 数量）
     */
    [[nodiscard]] unsigned getTupleSize() const noexcept;

    /**
     * @brief 获取第 i 个元组的指针
     * 
     * @param i 元组索引（从0开始）
     * @return 指向第 i 个元组的指针（指向 table 内部）
     * @throws std::runtime_error 如果索引越界
     */
    [[nodiscard]] unsigned* getTuple(unsigned i);

    /**
     * @brief 获取第 i 个元组的指针（const 版本）
     */
    [[nodiscard]] const unsigned* getTuple(unsigned i) const;

    /**
     * @brief 打印 Vector 内容（调试用）
     * 最多打印前10个元组
     */
    void print() const;

    /**
     * @brief 打印指定位置的元组
     * @param pos 元组起始位置（在 table 中的索引）
     */
    void printTuple(unsigned pos) const;

    /**
     * @brief 计算校验和
     * 对指定列的值进行求和（通过 rowId 访问）
     * 
     * @param col 列数据指针
     * @param rowIdPos 在元组中 rowId 的位置偏移
     * @return 校验和
     */
    [[nodiscard]] uint64_t checkSum(const uint64_t* col, unsigned rowIdPos) const;

    /**
     * @brief 清空 Vector 内容（释放 table，重置状态）
     * 与析构不同，此函数保留 Vector 对象，仅释放数据
     */
    void clear();

    // 公共成员变量（保持与原始C代码兼容）
    // 其他模块直接访问这些成员
    unsigned* table = nullptr;        ///< 存储元组的数组（通过 new 分配）
    unsigned tupleSize = 0;           ///< 每个元组包含的 rowId 数量
    unsigned nextPos = 0;             ///< 下一个可用位置（也是当前元素计数）
    unsigned capacity = 0;            ///< 数组容量（以 unsigned 为单位）
};

/**
 * @brief checkSum 线程函数的参数结构体
 */
struct CheckSumArg {
    Vector* vector;           ///< 目标 Vector
    const uint64_t* col;      ///< 列数据指针
    unsigned rowIdPos;        ///< rowId 在元组中的位置
    uint64_t* sum;            ///< 输出：校验和结果
};

/**
 * @brief checkSum 线程工作函数
 * 供 JobScheduler 调用的线程函数，计算部分校验和
 * 
 * @param arg 指向 CheckSumArg 的指针
 */
void checkSumFunc(void* arg);

/**
 * @brief 列相等性检查线程函数的参数结构体
 */
struct ColEqualityArg {
    Vector* newVector;        ///< 输出 Vector（存储满足条件的元组）
    Vector* oldVector;        ///< 输入 Vector（扫描源）
    const uint64_t* leftCol;  ///< 左列数据
    const uint64_t* rightCol; ///< 右列数据
    unsigned posLeft;         ///< 左 rowId 在元组中的位置
    unsigned posRight;        ///< 右 rowId 在元组中的位置
};

/**
 * @brief 列相等性检查线程工作函数
 * 供 JobScheduler 调用的线程函数，扫描满足 col1[rowId1] == col2[rowId2] 的元组
 * 
 * @param arg 指向 ColEqualityArg 的指针
 */
void colEqualityFunc(void* arg);

/**
 * @brief 扫描并过滤满足列相等性的元组
 * 将 old Vector 中满足 leftCol[rowIdLeft] == rightCol[rowIdRight] 的元组插入 new Vector
 * 
 * @param newVec 输出 Vector
 * @param oldVec 输入 Vector
 * @param leftCol 左列数据
 * @param rightCol 右列数据
 * @param posLeft 左 rowId 在元组中的位置
 * @param posRight 右 rowId 在元组中的位置
 */
void scanColEquality(Vector* newVec, Vector* oldVec, 
                     const uint64_t* leftCol, const uint64_t* rightCol,
                     unsigned posLeft, unsigned posRight);

/**
 * @brief 扫描并过滤满足比较条件的元组
 * 将 old Vector 中满足 compare(col[rowId], cmp, constant) 的元组插入 new Vector
 * 
 * @param newVec 输出 Vector
 * @param oldVec 输入 Vector
 * @param col 列数据
 * @param cmp 比较操作
 * @param constant 比较常量
 */
void scanFilter(Vector* newVec, Vector* oldVec, 
                const uint64_t* col, Comparison cmp, uint64_t constant);

/*==============================================================================
 * C 风格兼容函数 - 用于与 Partition/Build/Probe 模块兼容
 *============================================================================*/

// 前向声明
struct RadixHashJoinInfo;

/**
 * @brief 创建 Vector（C 风格兼容函数）
 * @param vector 指向 Vector* 的指针
 * @param tupleSize 每个元组的大小
 */
void createVector(Vector** vector, unsigned tupleSize);

/**
 * @brief 创建固定大小的 Vector（C 风格兼容函数）
 * @param vector 指向 Vector* 的指针
 * @param tupleSize 每个元组的大小
 * @param fixedSize 固定大小
 */
void createVectorFixedSize(Vector** vector, unsigned tupleSize, unsigned fixedSize);

/**
 * @brief 在 Vector 尾部插入元组（C 风格兼容函数）
 * @param vector 目标 Vector
 * @param tuple 要插入的元组
 */
void insertAtVector(Vector* vector, unsigned* tuple);

/**
 * @brief 在指定位置插入元组（C 风格兼容函数）
 * @param vector 目标 Vector
 * @param tuple 要插入的元组
 * @param offset 偏移位置
 */
void insertAtPos(Vector* vector, unsigned* tuple, unsigned offset);

/**
 * @brief 扫描 Join 操作（C 风格兼容函数）
 * 从中间结果中填充 unsorted 数据
 * @param joinRel RadixHashJoinInfo 指针
 */
void scanJoin(RadixHashJoinInfo* joinRel);

/**
 * @brief 销毁 Vector（C 风格兼容函数）
 * @param vector 指向 Vector* 的指针
 */
void destroyVector(Vector** vector);

/**
 * @brief 获取元组大小（C 风格兼容函数）
 * @param vector Vector 指针
 * @return 元组大小
 */
unsigned getTupleSize(Vector* vector);

} // namespace radix_join
