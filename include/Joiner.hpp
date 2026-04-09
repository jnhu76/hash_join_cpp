#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <memory>

#include "Relation.hpp"

namespace radix_join {

// 前向声明
class QueryInfo;

/**
 * @brief 哈希函数1 - 用于基数哈希连接的第一阶段
 * 计算键的低位作为哈希值
 * 
 * 使用全局变量 RADIX_BITS 来确定哈希范围
 */
#define HASH_FUN_1(KEY) ((KEY) & ((1u << RADIX_BITS) - 1))

/**
 * @brief 基数哈希连接的位数参数
 * 这些全局变量在Joiner.cpp中定义，被Partition、Build、Probe等模块引用
 */
extern unsigned RADIX_BITS;     ///< 基数哈希的位数（决定分区数量）
extern unsigned HASH_RANGE_1;   ///< 第一阶段哈希范围（2^RADIX_BITS）
extern unsigned initSize;       ///< Vector初始大小（根据平均元组数动态设置）

/**
 * @brief Joiner 类 - 管理所有关系和查询执行
 * 
 * Joiner是系统的核心组件，负责：
 * - 加载和管理所有关系（表）
 * - 设置哈希连接参数（根据数据大小）
 * - 执行查询（解析、优化、连接）
 */
class Joiner {
public:
    /**
     * @brief 默认构造函数
     * 创建一个空的Joiner
     */
    Joiner() = default;

    /**
     * @brief 从标准输入设置Joiner
     * 读取关系文件名列表（以"Done"结束），加载所有关系
     * 并根据数据大小设置RADIX_BITS和initSize
     */
    void setup();

    /**
     * @brief 添加关系
     * 从文件加载关系并添加到Joiner中
     * 
     * @param fileName 关系数据文件名
     */
    void addRelation(const std::string& fileName);

    /**
     * @brief 执行连接查询
     * 执行给定的查询并输出结果校验和到标准输出
     * 
     * @param q 查询信息
     */
    void join(const QueryInfo& q);

    /**
     * @brief 获取关系的元组数量
     * @param relId 关系ID
     * @return 元组数量
     */
    [[nodiscard]] unsigned getRelationTuples(unsigned relId) const {
        return relations_[relId]->numOfTuples;
    }

    /**
     * @brief 获取关系的列数据
     * @param relId 关系ID
     * @param colId 列ID
     * @return 列数据指针
     */
    [[nodiscard]] uint64_t* getColumn(unsigned relId, unsigned colId) const {
        return relations_[relId]->columns[colId];
    }

    /**
     * @brief 获取关系对象
     * @param relId 关系ID
     * @return 关系对象指针
     */
    [[nodiscard]] const Relation* getRelation(unsigned relId) const {
        return relations_[relId].get();
    }

    [[nodiscard]] Relation* getRelation(unsigned relId) {
        return relations_[relId].get();
    }

    /**
     * @brief 获取关系数量
     */
    [[nodiscard]] unsigned getNumOfRelations() const {
        return static_cast<unsigned>(relations_.size());
    }

    /**
     * @brief 设置RADIX_BITS参数
     * 根据关系的平均元组数设置合适的哈希位数
     * 
     * - small (< 500K):   RADIX_BITS=4, HASH_RANGE_1=16
     * - medium (< 2M):    RADIX_BITS=5, HASH_RANGE_1=32
     * - large (>= 2M):    RADIX_BITS=8, HASH_RANGE_1=256
     */
    void setRadixBits();

    /**
     * @brief 设置Vector初始大小
     * 根据关系的平均元组数设置Vector的初始容量
     * 
     * - small/medium: initSize=1000
     * - large: initSize=5000
     * - public: initSize=500000
     */
    void setVectorInitSize();

private:
    std::vector<std::unique_ptr<Relation>> relations_;  ///< 存储所有关系的智能指针数组
};

} // namespace radix_join
