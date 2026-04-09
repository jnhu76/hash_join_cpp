#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "Utils.hpp"
#include "Relation.hpp"

namespace radix_join {

// 前向声明
class Joiner;

/**
 * @brief 选择信息结构体
 * 表示查询中的选择操作，包含关系ID和列ID
 */
struct SelectInfo {
    unsigned relId;  ///< 关系ID（在查询中的位置索引）
    unsigned colId;  ///< 列ID
};

/**
 * @brief 过滤信息结构体
 * 表示查询中的过滤条件，如 r1.a > 100
 */
struct FilterInfo {
    SelectInfo filterLhs;  ///< 左侧选择信息（关系.列）
    Comparison comparison; ///< 比较操作符
    uint64_t constant;     ///< 常量值
};

/**
 * @brief 谓词信息结构体
 * 表示查询中的连接谓词，如 r1.a = r2.b
 */
struct PredicateInfo {
    SelectInfo left;   ///< 左侧选择信息
    SelectInfo right;  ///< 右侧选择信息
};

/**
 * @brief 查询信息类
 * 存储解析后的查询结构，包括关系、谓词、过滤器和选择项
 */
class QueryInfo {
public:
    std::vector<unsigned> relationIds;           ///< 查询涉及的关系ID列表
    std::vector<PredicateInfo> predicates;       ///< 连接谓词列表
    std::vector<FilterInfo> filters;             ///< 过滤条件列表
    std::vector<SelectInfo> selections;          ///< 选择项列表
    
    // 每个关系的估计统计信息（用于查询优化）
    std::vector<std::vector<ColumnStats>> estimations;

    /**
     * @brief 默认构造函数
     */
    QueryInfo() = default;

    /**
     * @brief 从原始查询字符串构造并解析
     * @param rawQuery 原始查询字符串，格式: "r1 r2 | r1.a=r2.b&r1.c>100 | r1.a r2.b"
     */
    explicit QueryInfo(const std::string& rawQuery);

    /**
     * @brief 解析查询字符串
     * @param rawQuery 原始查询字符串
     */
    void parseQuery(const std::string& rawQuery);

    /**
     * @brief 解析关系ID列表
     * @param rawRelations 关系ID字符串，如 "0 1 2"
     */
    void parseRelationIds(const std::string& rawRelations);

    /**
     * @brief 解析谓词列表
     * @param rawPredicates 谓词字符串，如 "0.1=1.2&0.2>100"
     */
    void parsePredicates(const std::string& rawPredicates);

    /**
     * @brief 解析选择项列表
     * @param rawSelections 选择项字符串，如 "0.1 1.2"
     */
    void parseSelections(const std::string& rawSelections);

    /**
     * @brief 创建查询估计统计信息
     * 从Joiner中复制各关系的统计信息用于查询优化
     * @param joiner Joiner对象
     */
    void createQueryEstimations(const Joiner& joiner);

    /**
     * @brief 获取原始关系ID
     * @param sInfo 选择信息
     * @return 原始关系ID
     */
    [[nodiscard]] unsigned getOriginalRelId(const SelectInfo& sInfo) const {
        return relationIds[sInfo.relId];
    }

    /**
     * @brief 获取关系ID
     */
    [[nodiscard]] static unsigned getRelId(const SelectInfo& sInfo) {
        return sInfo.relId;
    }

    /**
     * @brief 获取列ID
     */
    [[nodiscard]] static unsigned getColId(const SelectInfo& sInfo) {
        return sInfo.colId;
    }

    /**
     * @brief 获取常量值
     */
    [[nodiscard]] static uint64_t getConstant(const FilterInfo& fInfo) {
        return fInfo.constant;
    }

    /**
     * @brief 获取比较操作符
     */
    [[nodiscard]] static Comparison getComparison(const FilterInfo& fInfo) {
        return fInfo.comparison;
    }

    /**
     * @brief 获取关系数量
     */
    [[nodiscard]] unsigned getNumOfRelations() const {
        return static_cast<unsigned>(relationIds.size());
    }

    /**
     * @brief 获取过滤器数量
     */
    [[nodiscard]] unsigned getNumOfFilters() const {
        return static_cast<unsigned>(filters.size());
    }

    /**
     * @brief 获取列等值谓词数量（同一关系的不同列之间的等值比较）
     */
    [[nodiscard]] unsigned getNumOfColEqualities() const;

    /**
     * @brief 获取连接谓词数量（不同关系之间的等值比较）
     */
    [[nodiscard]] unsigned getNumOfJoins() const;

    /**
     * @brief 打印查询信息（调试用）
     */
    void print() const;
    
    /**
     * @brief 析构函数
     * 释放 estimations 中分配的 bitVector 内存
     */
    ~QueryInfo();
};

/**
 * @brief 判断谓词是否为过滤器（与常量比较）
 * @param predicate 谓词字符串
 * @return true 如果是过滤器（如 "0.1>100"）
 * @return false 如果是连接谓词（如 "0.1=1.2"）
 */
[[nodiscard]] bool isFilter(const std::string& predicate);

/**
 * @brief 判断谓词是否为同一关系内的列等值比较
 * @param pInfo 谓词信息
 * @return true 如果是列等值（如 r1.a = r1.b）
 */
[[nodiscard]] inline bool isColEquality(const PredicateInfo& pInfo) {
    return pInfo.left.relId == pInfo.right.relId;
}

/**
 * @brief 添加过滤器信息
 * @param fInfo 过滤器信息结构体（输出）
 * @param token 谓词字符串标记
 */
void addFilter(FilterInfo& fInfo, const std::string& token);

/**
 * @brief 添加谓词信息
 * @param pInfo 谓词信息结构体（输出）
 * @param token 谓词字符串标记
 */
void addPredicate(PredicateInfo& pInfo, const std::string& token);

} // namespace radix_join
