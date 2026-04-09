#include <cstdio>
#include <cstdlib>
#include <cstring>   // for calloc/memcpy
#include <cmath>
#include <iostream>

#include "Optimizer.hpp"
#include "Joiner.hpp"

namespace radix_join {

// 内部浮点power函数 - 与C版本兼容（原C代码中使用浮点幂运算）
static double floatPower(double base, double exponent) {
    if (exponent == 0.0) return 1.0;
    return std::pow(base, exponent);
}

void findStats(const uint64_t* column, ColumnStats* stat, unsigned columnSize) {
    // 处理空输入情况
    if (columnSize == 0 || column == nullptr) {
        stat->minValue = 0;
        stat->maxValue = 0;
        stat->f = 0;
        stat->discreteValues = 0;
        stat->bitVectorSize = 0;
        stat->typeOfBitVector = 0;
        stat->bitVector = nullptr;
        return;
    }
    
    // 查找最小值和最大值
    uint64_t min = column[0];
    uint64_t max = column[0];
    for (unsigned i = 1; i < columnSize; ++i) {
        if (column[i] > max) max = column[i];
        if (column[i] < min) min = column[i];
    }
    
    // 计算不同值的数量（使用位向量）
    // 处理极端值情况，避免max - min + 1溢出
    unsigned nbits;
    bool useHashMode = false;
    // 直接检查max和min的差值是否过大
    if (max - min > static_cast<uint64_t>(PRIMELIMIT)) {
        nbits = PRIMELIMIT;
        useHashMode = true;
    } else {
        nbits = static_cast<unsigned>(max - min + 1);
        useHashMode = false;
    }
    
    // 限制位向量大小，避免分配过多内存
    if (nbits > 1000000) { // 限制为100万
        nbits = 1000000;
        useHashMode = true; // 限制大小后使用哈希模式
    }
    
    // 先释放已有的bitVector内存
    if (stat->bitVector) {
        delete[] stat->bitVector;
        stat->bitVector = nullptr;
    }
    
    // 分配位向量内存（new[]确保初始为0）
    stat->bitVector = new char[BITNSLOTS(nbits)]();
    if (!stat->bitVector) {
        throw std::runtime_error("Failed to allocate bit vector memory");
    }
    stat->discreteValues = 0;
    
    // 根据是否需要哈希模式，选择填充方式
    if (useHashMode) {
        stat->typeOfBitVector = 1;  // 哈希模式（取模）
        for (unsigned i = 0; i < columnSize; ++i) {
            // 计算哈希位置，确保不会溢出
            uint64_t value = column[i] - min;
            unsigned bitPos = static_cast<unsigned>(value % nbits);
            if (BITTEST(stat->bitVector, bitPos) == 0) {
                ++(stat->discreteValues);
            }
            BITSET(stat->bitVector, bitPos);
        }
    } else {
        stat->typeOfBitVector = 0;  // 精确模式
        for (unsigned i = 0; i < columnSize; ++i) {
            // 确保位位置在有效范围内
            uint64_t value = column[i] - min;
            if (value < static_cast<uint64_t>(nbits)) {
                unsigned bitPos = static_cast<unsigned>(value);
                if (BITTEST(stat->bitVector, bitPos) == 0) {
                    ++(stat->discreteValues);
                }
                BITSET(stat->bitVector, bitPos);
            }
        }
    }
    
    // 赋值统计结果
    stat->minValue      = min;
    stat->maxValue      = max;
    stat->f             = columnSize;
    stat->bitVectorSize = nbits;
}

void applyColEqualityEstimations(QueryInfo& q, const Joiner& j) {
    for (unsigned i = 0; i < q.predicates.size(); ++i) {
        unsigned leftRelId  = QueryInfo::getRelId(q.predicates[i].left);
        unsigned rightRelId = QueryInfo::getRelId(q.predicates[i].right);
        unsigned leftColId  = QueryInfo::getColId(q.predicates[i].left);
        unsigned rightColId = QueryInfo::getColId(q.predicates[i].right);
        unsigned actualId   = q.getOriginalRelId(q.predicates[i].left);
        
        ColumnStats* stat1 = &q.estimations[leftRelId][leftColId];
        ColumnStats* stat2 = &q.estimations[rightRelId][rightColId];
        ColumnStats* temp;
        
        // 同一关系的不同列之间的等值比较
        if (isColEquality(q.predicates[i]) && (leftColId != rightColId)) {
            // 计算等值后的统计估计
            uint64_t newMin = (stat1->minValue > stat2->minValue) ? stat1->minValue : stat2->minValue;
            uint64_t newMax = (stat1->maxValue < stat2->maxValue) ? stat1->maxValue : stat2->maxValue;
            unsigned newF   = stat1->f / (newMax - newMin + 1);
            unsigned newD   = static_cast<unsigned>(
                stat1->discreteValues * (1.0 - floatPower(1.0 - static_cast<double>(newF) / stat1->f,
                    static_cast<double>(stat1->f) / stat1->discreteValues))
            );
            
            // 更新该关系中其他列的统计信息
            unsigned numCols = j.getRelation(actualId)->numOfCols;
            for (unsigned c = 0; c < numCols; ++c) {
                temp = &q.estimations[leftRelId][c];
                if ((c != leftColId) && (c != rightColId)) {
                    // 防止除以零
                    unsigned safeF = (stat1->f == 0) ? 1 : stat1->f;
                    unsigned safeD = (temp->discreteValues == 0) ? 1 : temp->discreteValues;
                    
                    temp->discreteValues = static_cast<unsigned>(
                        safeD * (1.0 - floatPower(1.0 - static_cast<double>(newF) / safeF,
                            static_cast<double>(temp->f) / safeD))
                    );
                }
            }
            
            // 更新参与等值比较的两列的统计信息
            stat1->minValue       = stat2->minValue       = newMin;
            stat1->maxValue       = stat2->maxValue       = newMax;
            stat1->f              = stat2->f              = newF;
            stat1->discreteValues = stat2->discreteValues = newD;
        }
        // 同一关系同一列的等值比较（自比较）
        else if (isColEquality(q.predicates[i])) {
            stat1->f = (stat1->f * stat1->f) / (stat1->maxValue - stat1->minValue + 1);
            unsigned numCols = j.getRelation(actualId)->numOfCols;
            for (unsigned c = 0; c < numCols; ++c) {
                temp = &q.estimations[leftRelId][c];
                if (c != rightColId) {
                    temp->f = stat1->f;
                }
            }
        }
    }
}

void applyFilterEstimations(QueryInfo& q, const Joiner& j) {
    std::cerr << "[DEBUG] applyFilterEstimations: filters.size()=" << q.filters.size() << std::endl;
    for (unsigned i = 0; i < q.filters.size(); ++i) {
        unsigned relId       = QueryInfo::getRelId(q.filters[i].filterLhs);
        unsigned colId       = QueryInfo::getColId(q.filters[i].filterLhs);
        Comparison cmp       = QueryInfo::getComparison(q.filters[i]);
        uint64_t constant    = QueryInfo::getConstant(q.filters[i]);
        unsigned actualRelId = q.getOriginalRelId(q.filters[i].filterLhs);
        
        std::cerr << "[DEBUG] Filter " << i << ": relId=" << relId << ", colId=" << colId 
                  << ", actualRelId=" << actualRelId << std::endl;
        
        ColumnStats* stat    = &q.estimations[relId][colId];
        
        std::cerr << "[DEBUG] Calling filterEstimation..." << std::endl;
        filterEstimation(j, q, colId, stat, actualRelId, relId, cmp, constant);
        std::cerr << "[DEBUG] filterEstimation done" << std::endl;
    }
}

void applyJoinEstimations(QueryInfo& q, const Joiner& j) {
    std::cerr << "[DEBUG] applyJoinEstimations: predicates.size()=" << q.predicates.size() << std::endl;
    // 计算各谓词的统计信息
    for (unsigned i = 0; i < q.predicates.size(); ++i) {
        std::cerr << "[DEBUG] Processing predicate " << i << std::endl;
        unsigned leftRelId   = QueryInfo::getRelId(q.predicates[i].left);
        unsigned rightRelId  = QueryInfo::getRelId(q.predicates[i].right);
        unsigned leftColId   = QueryInfo::getColId(q.predicates[i].left);
        unsigned rightColId  = QueryInfo::getColId(q.predicates[i].right);
        unsigned actualRelIdLeft  = q.getOriginalRelId(q.predicates[i].left);
        unsigned actualRelIdRight = q.getOriginalRelId(q.predicates[i].right);
        
        std::cerr << "[DEBUG] leftRelId=" << leftRelId << ", rightRelId=" << rightRelId 
                  << ", leftColId=" << leftColId << ", rightColId=" << rightColId << std::endl;
        
        std::cerr << "[DEBUG] estimations.size()=" << q.estimations.size() << std::endl;
        if (leftRelId < q.estimations.size()) {
            std::cerr << "[DEBUG] estimations[" << leftRelId << "].size()=" << q.estimations[leftRelId].size() << std::endl;
        }
        if (rightRelId < q.estimations.size()) {
            std::cerr << "[DEBUG] estimations[" << rightRelId << "].size()=" << q.estimations[rightRelId].size() << std::endl;
        }
        
        ColumnStats* statLeft  = &q.estimations[leftRelId][leftColId];
        ColumnStats* statRight = &q.estimations[rightRelId][rightColId];
        ColumnStats* temp;
        
        // 不同关系之间的连接
        if (!isColEquality(q.predicates[i])) {
            // 保存旧的不同值数量（用于更新其他列）
            unsigned oldDiscreteLeft  = statLeft->discreteValues;
            unsigned oldDiscreteRight = statRight->discreteValues;
            
            // 计算连接范围的min/max
            uint64_t min = (statLeft->minValue > statRight->minValue) ? statLeft->minValue : statRight->minValue;
            uint64_t max = (statLeft->maxValue < statRight->maxValue) ? statLeft->maxValue : statRight->maxValue;
            
            // 先应用隐式过滤器（最小值过滤）
            filterEstimation(j, q, leftColId, statLeft, actualRelIdLeft, leftRelId, Comparison::Greater, min);
            filterEstimation(j, q, rightColId, statRight, actualRelIdRight, rightRelId, Comparison::Greater, min);
            
            // 应用隐式过滤器（最大值过滤）
            filterEstimation(j, q, leftColId, statLeft, actualRelIdLeft, leftRelId, Comparison::Less, max);
            filterEstimation(j, q, rightColId, statRight, actualRelIdRight, rightRelId, Comparison::Less, max);
            
            // 计算连接后的统计信息
            statLeft->f              = statRight->f              = (statLeft->f * statRight->f) / (max - min + 1);
            statLeft->minValue       = statRight->minValue       = min;
            statLeft->maxValue       = statRight->maxValue       = max;
            statLeft->discreteValues = statRight->discreteValues = 
                (statLeft->discreteValues * statRight->discreteValues) / (max - min + 1);
            
            // 更新左关系中其他列的统计信息
            unsigned numColsLeft = j.getRelation(actualRelIdLeft)->numOfCols;
            for (unsigned c = 0; c < numColsLeft; ++c) {
                temp = &q.estimations[leftRelId][c];
                if (c != leftColId) {
                    oldDiscreteLeft  = (oldDiscreteLeft == 0) ? 1 : oldDiscreteLeft;
                    unsigned safeD   = (temp->discreteValues == 0) ? 1 : temp->discreteValues;
                    
                    temp->f = statLeft->f;
                    temp->discreteValues = static_cast<unsigned>(
                        safeD * (1.0 - floatPower(1.0 - static_cast<double>(statLeft->discreteValues) / oldDiscreteLeft,
                            static_cast<double>(temp->f) / safeD))
                    );
                }
            }
            
            // 更新右关系中其他列的统计信息
            unsigned numColsRight = j.getRelation(actualRelIdRight)->numOfCols;
            for (unsigned c = 0; c < numColsRight; ++c) {
                temp = &q.estimations[rightRelId][c];
                if (c != rightColId) {
                    oldDiscreteRight = (oldDiscreteRight == 0) ? 1 : oldDiscreteRight;
                    unsigned safeD   = (temp->discreteValues == 0) ? 1 : temp->discreteValues;
                    
                    temp->f = statLeft->f;
                    temp->discreteValues = static_cast<unsigned>(
                        safeD * (1.0 - floatPower(1.0 - static_cast<double>(statRight->discreteValues) / oldDiscreteRight,
                            static_cast<double>(temp->f) / safeD))
                    );
                }
            }
        }
    }
}

void filterEstimation(const Joiner& j, QueryInfo& q, unsigned colId, ColumnStats* stat,
                      unsigned actualRelId, unsigned relId, Comparison cmp, uint64_t constant) {
    std::cerr << "[DEBUG] filterEstimation: colId=" << colId << ", actualRelId=" << actualRelId 
              << ", relId=" << relId << std::endl;
    
    ColumnStats* temp;
    uint64_t fTemp   = stat->f;  // 保存更新前的频率
    char isInArray   = 0;
    
    std::cerr << "[DEBUG] fTemp=" << fTemp << ", cmp=" << static_cast<int>(cmp) << ", constant=" << constant << std::endl;
    
    if (cmp == Comparison::Equal) {
        // 等值过滤：检查常量是否在位向量中
        if ((constant < stat->minValue) || (constant > stat->maxValue)) {
            isInArray = 0;
        } else if (stat->bitVector != nullptr) {
            // 根据位向量类型检查常量是否存在
            if (stat->typeOfBitVector == 0) {
                unsigned bitPos = static_cast<unsigned>(constant - stat->minValue);
                if (bitPos < stat->bitVectorSize) {
                    unsigned slot = BITSLOT(bitPos);
                    if (slot < BITNSLOTS(stat->bitVectorSize)) {
                        if (BITTEST(stat->bitVector, bitPos) != 0) {
                            isInArray = 1;
                        }
                    }
                }
            } else {
                unsigned bitPos = static_cast<unsigned>((constant - stat->minValue) % PRIMELIMIT);
                if (bitPos < PRIMELIMIT) {
                    unsigned slot = BITSLOT(bitPos);
                    if (slot < BITNSLOTS(PRIMELIMIT)) {
                        if (BITTEST(stat->bitVector, bitPos) != 0) {
                            isInArray = 1;
                        }
                    }
                }
            }
        } else {
            // 如果bitVector为nullptr，默认认为常量不在数组中
            isInArray = 0;
        }
        
        // 更新列统计信息
        stat->minValue = constant;
        stat->maxValue = constant;
        if (isInArray == 0) {
            stat->f = 0;
            stat->discreteValues = 0;
        } else {
            stat->f = stat->f / stat->discreteValues;
            stat->discreteValues = 1;
        }
    } else {
        // 不等式过滤（< 或 >）
        // 计算上下限
        uint64_t k1 = (cmp == Comparison::Greater) ? constant + 1 : stat->minValue;
        uint64_t k2 = (cmp == Comparison::Less) ? constant - 1 : stat->maxValue;
        
        // 截断到合法范围
        if (k1 < stat->minValue) k1 = stat->minValue;
        if (k2 > stat->maxValue) k2 = stat->maxValue;
        
        // 计算选择率（如果在0-1之间且大于0则取1）
        double factor = (double)(k2 - k1) / (stat->maxValue - stat->minValue);
        if ((factor <= 1) && (factor > 0)) {
            factor = 1;
        }
        
        // 根据范围更新统计信息
        if (stat->maxValue - stat->minValue > 0) {
            stat->discreteValues = static_cast<unsigned>(factor * stat->discreteValues);
            stat->f              = static_cast<unsigned>(factor * stat->f);
        } else {
            // 当min==max但过滤器要求不等式时，结果为空
            stat->discreteValues = stat->f = 0;
        }
        
        stat->minValue = k1;
        stat->maxValue = k2;
    }
    
    // 更新该关系中其他所有列的统计信息
    unsigned numCols = j.getRelation(actualRelId)->numOfCols;
    for (unsigned c = 0; c < numCols; ++c) {
        temp = &q.estimations[relId][c];
        // 只更新非当前列
        if (c != colId) {
            // 防止除以零
            uint64_t safeFTemp = (fTemp == 0) ? 1 : fTemp;
            unsigned safeD     = (temp->discreteValues == 0) ? 1 : temp->discreteValues;
            
            temp->discreteValues = static_cast<unsigned>(
                safeD * (1.0 - floatPower(
                    1.0 - (1.0 - static_cast<double>(stat->f) / safeFTemp),
                    static_cast<double>(temp->f) / safeD
                ))
            );
            temp->f = stat->f;
        }
    }
}

void findOptimalJoinOrder(QueryInfo& q, const Joiner& j) {
    // 当前实现为空函数，保留扩展接口
    // 未来可以根据统计信息实现连接顺序优化
    (void)q;
    (void)j;
}

void columnPrint(const uint64_t* column, unsigned columnSize) {
    for (unsigned i = 0; i < columnSize; ++i) {
        std::cerr << column[i] << " ";
    }
    std::cerr << "\n";
}

void printBooleanArray(const char* array, unsigned size) {
    for (unsigned i = 0; i < size; ++i) {
        std::cerr << static_cast<int>(array[i]) << " ";
    }
    std::cerr << "\n";
}

void printColumnStats(const ColumnStats* s) {
    std::cerr << "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n";
    std::cerr << "minValue: " << s->minValue << "\n";
    std::cerr << "maxValue: " << s->maxValue << "\n";
    std::cerr << "f: " << s->f << "\n";
    std::cerr << "discreteValues: " << s->discreteValues << "\n";
    std::cerr << "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n";
}

} // namespace radix_join
