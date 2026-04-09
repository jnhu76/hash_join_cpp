#pragma once

#include <cstdint>
#include <cstddef>
#include <string>

namespace radix_join {

/**
 * @brief 列统计信息结构体
 * 用于查询优化器估计代价和选择最优执行计划
 */
struct ColumnStats {
    uint64_t minValue;        ///< 列中最小值
    uint64_t maxValue;        ///< 列中最大值
    unsigned f;               ///< 频率/基数估计
    unsigned discreteValues;  ///< 不同值的数量
    char* bitVector;          ///< 位图（用于快速判断值是否存在）
    unsigned bitVectorSize;   ///< 位图大小（字节）
    char typeOfBitVector;     ///< 位图类型标记
    
    /**
     * @brief 默认构造函数
     * 初始化所有成员变量
     */
    ColumnStats() : 
        minValue(0),
        maxValue(0),
        f(0),
        discreteValues(0),
        bitVector(nullptr),
        bitVectorSize(0),
        typeOfBitVector(0) {}
};

/**
 * @brief Relation 类 - 数据库关系（表）
 * 
 * 该类表示一个数据库关系，包含多个列（columns）。
 * 数据通过 mmap 从文件加载，以获得最佳性能。
 * 
 * 关键设计：
 * - 使用 mmap 加载数据（性能关键路径）
 * - columns 是 uint64_t** 数组，每列是一个 uint64_t 数组
 * - 使用 RAII 管理资源，但注意 mmap 的内存由操作系统管理
 * - 统计信息用于查询优化
 */
class Relation {
public:
    /**
     * @brief 默认构造函数
     * 创建一个空的 Relation
     */
    Relation() = default;

    /**
     * @brief 从文件加载关系的构造函数
     * 
     * @param fileName 关系数据文件路径
     */
    explicit Relation(const std::string& fileName);

    /**
     * @brief 析构函数
     * 释放统计信息和列指针数组（注意：mmap 的内存由 OS 回收）
     */
    ~Relation();

    // 禁止拷贝
    Relation(const Relation&) = delete;
    Relation& operator=(const Relation&) = delete;

    // 允许移动语义
    Relation(Relation&& other) noexcept;
    Relation& operator=(Relation&& other) noexcept;

    /**
     * @brief 从文件加载关系数据
     * 使用 mmap 将文件映射到内存，实现零拷贝数据访问
     * 
     * @param fileName 关系数据文件路径
     * @throws std::runtime_error 如果文件打开或 mmap 失败
     */
    void loadFromFile(const std::string& fileName);

    /**
     * @brief 将关系数据导出到文件
     * 使用 C++ 文件流将数据以文本格式导出
     * 
     * @param fileName 输出文件名（不含路径和扩展名）
     * @throws std::runtime_error 如果文件创建失败
     */
    void dumpToFile(const std::string& fileName) const;

    /**
     * @brief 打印关系内容到标准输出（调试用）
     */
    void print() const;

    /**
     * @brief 计算并设置所有列的统计信息
     * 调用 findStats 函数为每列计算最小值、最大值、不同值数量等
     */
    void calculateStats();

    // 公共成员变量（保持与原始C代码兼容）
    unsigned numOfTuples = 0;      ///< 元组（行）数量
    unsigned numOfCols = 0;        ///< 列数量
    uint64_t** columns = nullptr;  ///< 列数据指针数组（每列是一个 uint64_t 数组）
    ColumnStats* stats = nullptr;  ///< 每列的统计信息数组

private:
    /**
     * @brief 计算单列的统计信息
     * 
     * @param column 列数据指针
     * @param stat 输出统计信息结构体
     * @param columnSize 列中元素数量
     */
    void findStats(const uint64_t* column, ColumnStats* stat, unsigned columnSize);
    
    // mmap相关成员，用于显式释放内存
    char* mmapAddr = nullptr;         // mmap的起始地址
    size_t mmapSize = 0;              // mmap的大小
};

/**
 * @brief 从文件加载关系（独立函数，兼容原始C接口风格）
 * 
 * @param fileName 关系数据文件路径
 * @return 加载好的 Relation 对象
 * @throws std::runtime_error 如果加载失败
 */
[[nodiscard]] Relation loadRelation(const std::string& fileName);

/**
 * @brief 将关系导出到文件（独立函数，兼容原始C接口风格）
 * 
 * @param rel 要导出的关系
 * @param fileName 输出文件名（不含路径和扩展名）
 */
void dumpRelation(const Relation& rel, const std::string& fileName);

/**
 * @brief 打印关系内容（独立函数，兼容原始C接口风格）
 * 
 * @param rel 要打印的关系
 */
void printRelation(const Relation& rel);

} // namespace radix_join
