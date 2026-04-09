/*==============================================================================
 * Intermediate.cpp - 中间结果管理模块
 * 
 * 该模块负责管理查询执行过程中的中间结果，包括：
 * - 中间结果向量的创建、管理和销毁
 * - 列等值谓词的应用（同一关系内的列比较）
 * - 过滤条件的应用
 * - 连接操作的执行（四种 join 策略）
 * - 校验和的计算
 * 
 * 转换说明（C -> C++20）：
 * - malloc/free 替换为 new/delete
 * - 三维指针 Vector*** 和二维指针 unsigned** 使用 new 分配
 * - 保持与 JobScheduler 全局变量的交互模式
 * - 保持与 Partition/Build/Probe 的调用关系
 *============================================================================*/

#include <cstdio>
#include <cstring>
#include <unistd.h>
#include <mutex>
#include <condition_variable>

#include "Intermediate.hpp"
#include "Parser.hpp"
#include "Partition.hpp"
#include "Operations.hpp"
#include "Utils.hpp"
#include "Vector.hpp"
#include "Queue.hpp"
#include "JobScheduler.hpp"
#include "Joiner.hpp"

namespace radix_join {

/*==============================================================================
 * 创建器/初始化器
 *============================================================================*/

/**
 * @brief 创建中间结果元数据
 * 
 * 分配 InterMetaData 结构及其内部数组：
 * - interResults: 三维数组 [maxNumOfVectors][HASH_RANGE_1]
 * - mapRels: 二维数组 [maxNumOfVectors]
 */
void createInterMetaData(InterMetaData** inter, QueryInfo* q) {
    // 分配主结构
    *inter = new InterMetaData();
    
    // 计算可能需要的最大向量数量
    (*inter)->maxNumOfVectors = q->getNumOfFilters() + q->getNumOfColEqualities() + q->getNumOfJoins();
    (*inter)->queryRelations = q->getNumOfRelations();
    
    // 分配 interResults 三维数组
    (*inter)->interResults = new Vector**[(*inter)->maxNumOfVectors];
    for (unsigned i = 0; i < (*inter)->maxNumOfVectors; ++i) {
        (*inter)->interResults[i] = new Vector*[HASH_RANGE_1];
        for (unsigned v = 0; v < HASH_RANGE_1; ++v) {
            (*inter)->interResults[i][v] = nullptr;
        }
    }
    
    // 分配 mapRels 二维数组
    (*inter)->mapRels = new unsigned*[(*inter)->maxNumOfVectors];
    for (unsigned i = 0; i < (*inter)->maxNumOfVectors; ++i) {
        (*inter)->mapRels[i] = nullptr;
    }
}

/**
 * @brief 初始化 RadixHashJoinInfo 结构
 * 
 * 根据选择信息填充 RadixHashJoinInfo 结构，包括：
 * - 关系/列标识
 * - 原始列数据或中间结果向量
 * - 映射数组
 * - 元组数量和大小
 */
void initializeInfo(InterMetaData* inter, QueryInfo* q, SelectInfo* s, Joiner* j, RadixHashJoinInfo* arg) {
    arg->relId = QueryInfo::getRelId(*s);
    arg->colId = QueryInfo::getColId(*s);
    arg->col = j->getColumn(q->getOriginalRelId(*s), arg->colId);
    
    unsigned pos = getVectorPos(inter, arg->relId);
    arg->vector = inter->interResults[pos];
    arg->map = inter->mapRels[pos];
    arg->queryRelations = inter->queryRelations;
    arg->ptrToVec = inter->interResults[pos] + 0;
    arg->ptrToMap = &inter->mapRels[pos];
    arg->pos = pos;
    
    // 判断关系是否在中间结果中
    if (isInInter(arg->vector[0])) {
        arg->isInInter = 1;
        arg->numOfTuples = 0;
        // 统计所有分区中的元组数量
        for (unsigned i = 0; i < HASH_RANGE_1; ++i) {
            if (arg->vector[i]) {
                arg->numOfTuples += arg->vector[i]->getTupleCount();
            }
        }
        arg->tupleSize = arg->vector[0]->getTupleSize();
    } else {
        arg->isInInter = 0;
        arg->numOfTuples = j->getRelationTuples(q->getOriginalRelId(*s));
        arg->tupleSize = 1;
    }
}

/*==============================================================================
 * 应用函数 - 查询执行主流程
 *============================================================================*/

/**
 * @brief 应用列等值谓词（同一关系内的列比较，如 r1.a = r1.b）
 * 
 * 遍历所有谓词，对于同一关系内的列等值比较：
 * - 如果关系已在中间结果中，使用 colEqualityInter 进行过滤
 * - 否则，使用 colEquality 创建新的中间结果
 */
void applyColumnEqualities(InterMetaData* inter, Joiner* joiner, QueryInfo* q) {
    for (const auto& predicate : q->predicates) {
        if (isColEquality(predicate)) {
            unsigned original = q->getOriginalRelId(predicate.left);
            unsigned relId = QueryInfo::getRelId(predicate.left);
            unsigned leftColId = QueryInfo::getColId(predicate.left);
            unsigned rightColId = QueryInfo::getColId(predicate.right);
            unsigned pos = getVectorPos(inter, relId);
            Vector** vector = inter->interResults[pos] + 0;
            unsigned numOfTuples = joiner->getRelationTuples(original);
            uint64_t* leftCol = joiner->getColumn(original, leftColId);
            uint64_t* rightCol = joiner->getColumn(original, rightColId);
            
            if (isInInter(vector[0])) {
                // 关系已在中间结果中，进行过滤
                colEqualityInter(leftCol, rightCol, 0, 0, vector);
            } else {
                // 关系不在中间结果中，创建新的中间结果
                unsigned* values = new unsigned[inter->queryRelations];
                for (unsigned i = 0; i < inter->queryRelations; ++i) {
                    values[i] = (i == relId) ? 0 : static_cast<unsigned>(-1);
                }
                createMap(&inter->mapRels[pos], inter->queryRelations, values);
                delete[] values;
                colEquality(leftCol, rightCol, numOfTuples, vector);
            }
        }
    }
}

/**
 * @brief 应用过滤条件
 * 
 * 遍历所有过滤器，对于每个过滤条件：
 * - 如果关系已在中间结果中，使用 filterInter 进行过滤
 * - 否则，提交并行过滤任务到 JobScheduler
 */
void applyFilters(InterMetaData* inter, Joiner* joiner, QueryInfo* q) {
    for (const auto& filter : q->filters) {
        unsigned original = q->getOriginalRelId(filter.filterLhs);
        unsigned relId = QueryInfo::getRelId(filter.filterLhs);
        unsigned colId = QueryInfo::getColId(filter.filterLhs);
        uint64_t constant = QueryInfo::getConstant(filter);
        Comparison cmp = QueryInfo::getComparison(filter);
        unsigned pos = getVectorPos(inter, relId);
        Vector** vector = inter->interResults[pos];
        unsigned numOfTuples = joiner->getRelationTuples(original);
        uint64_t* col = joiner->getColumn(original, colId);
        
        if (isInInter(vector[0])) {
            // 关系已在中间结果中，进行过滤
            filterInter(col, cmp, constant, vector);
        } else {
            // 关系不在中间结果中，创建映射数组
            unsigned* values = new unsigned[inter->queryRelations];
            for (unsigned i = 0; i < inter->queryRelations; ++i) {
                values[i] = (i == relId) ? 0 : static_cast<unsigned>(-1);
            }
            createMap(&inter->mapRels[pos], inter->queryRelations, values);
            delete[] values;
            
            // 添加过滤任务到队列
            jobsFinished = 0;
            unsigned chunkSize = numOfTuples / HASH_RANGE_1;
            unsigned lastEnd = 0;
            unsigned i;
            
            for (i = 0; i < HASH_RANGE_1 - 1; ++i) {
                FilterArg* arg = static_cast<FilterArg*>(js->filterJobs[i].argument);
                arg->col = col;
                arg->constant = constant;
                arg->cmp = cmp;
                arg->start = i * chunkSize;
                arg->end = arg->start + chunkSize;
                arg->vector = vector + i;
                lastEnd = arg->end;
                
                std::unique_lock<std::mutex> lock(queueMtx);
                [[maybe_unused]] bool enqueueResult = jobQueue->enQueue(&js->filterJobs[i]);
                condNonEmpty.notify_one();
            }
            
            // 最后一个任务处理剩余数据
            FilterArg* arg = static_cast<FilterArg*>(js->filterJobs[i].argument);
            arg->col = col;
            arg->constant = constant;
            arg->cmp = cmp;
            arg->start = lastEnd;
            arg->end = numOfTuples;
            arg->vector = vector + i;
            
            std::unique_lock<std::mutex> lock(queueMtx);
            [[maybe_unused]] bool enqueueResult = jobQueue->enQueue(&js->filterJobs[i]);
            condNonEmpty.notify_one();
            lock.unlock();
            
            // 等待所有过滤任务完成
            std::unique_lock<std::mutex> jobsLock(jobsFinishedMtx);
            while (jobsFinished != HASH_RANGE_1) {
                condJobsFinished.wait(jobsLock);
            }
            jobsFinished = 0;
            condJobsFinished.notify_one();
        }
    }
}

/**
 * @brief 应用连接操作
 * 
 * 遍历所有谓词，对于非列等值谓词（即真正的连接谓词）：
 * - 初始化左右关系信息
 * - 调用 applyProperJoin 执行实际的连接
 */
void applyJoins(InterMetaData* inter, Joiner* joiner, QueryInfo* q) {
    for (const auto& predicate : q->predicates) {
        if (!isColEquality(predicate)) {
            RadixHashJoinInfo argLeft, argRight;
            initializeInfo(inter, q, const_cast<SelectInfo*>(&predicate.left), joiner, &argLeft);
            initializeInfo(inter, q, const_cast<SelectInfo*>(&predicate.right), joiner, &argRight);
            applyProperJoin(inter, &argLeft, &argRight);
        }
    }
}

/**
 * @brief 执行实际的连接操作
 * 
 * 根据左右关系是否在中间结果中，选择四种 join 策略之一：
 * - joinNonInterNonInter: 两个关系都不在中间结果中
 * - joinNonInterInter: 左关系不在，右关系在中间结果中
 * - joinInterNonInter: 左关系在中间结果中，右关系不在
 * - joinInterInter: 两个关系都在中间结果中
 */
void applyProperJoin(InterMetaData* inter, RadixHashJoinInfo* argLeft, RadixHashJoinInfo* argRight) {
    switch (argLeft->isInInter) {
        case 0:
            switch (argRight->isInInter) {
                case 0:
                    joinNonInterNonInter(inter, argLeft, argRight);
                    break;
                case 1:
                    joinNonInterInter(inter, argLeft, argRight);
                    break;
            }
            break;
        case 1:
            switch (argRight->isInInter) {
                case 0:
                    joinInterNonInter(inter, argLeft, argRight);
                    break;
                case 1:
                    joinInterInter(inter, argLeft, argRight);
                    break;
            }
            break;
    }
}

/**
 * @brief 计算并输出校验和
 * 
 * 对于每个选择项：
 * - 如果关系在中间结果中，提交并行校验和计算任务
 * - 否则，输出 NULL
 */
void applyCheckSums(InterMetaData* inter, Joiner* joiner, QueryInfo* q) {
    unsigned numSelections = static_cast<unsigned>(q->selections.size());
    for (unsigned i = 0; i < numSelections; ++i) {
        unsigned original = q->getOriginalRelId(q->selections[i]);
        unsigned relId = QueryInfo::getRelId(q->selections[i]);
        unsigned colId = QueryInfo::getColId(q->selections[i]);
        Vector** vector = inter->interResults[getVectorPos(inter, relId)];
        unsigned* rowIdMap = inter->mapRels[getVectorPos(inter, relId)];
        uint64_t* col = joiner->getColumn(original, colId);
        unsigned isLast = (i == numSelections - 1) ? 1 : 0;
        
        // 如果关系没有参与任何谓词/过滤器
        if (!isInInter(vector[0])) {
            printCheckSum(0, isLast);
        } else {
            // 添加校验和任务到队列
            for (unsigned j = 0; j < HASH_RANGE_1; ++j) {
                CheckSumArg* arg = static_cast<CheckSumArg*>(js->checkSumJobs[j].argument);
                arg->vector = vector[j];
                arg->col = col;
                arg->rowIdPos = rowIdMap[relId];
                arg->sum = &js->checkSumArray[j];
                
                std::unique_lock<std::mutex> lock(queueMtx);
                [[maybe_unused]] bool enqueueResult = jobQueue->enQueue(&js->checkSumJobs[j]);
                condNonEmpty.notify_one();
            }
            
            // 等待所有校验和任务完成
            std::unique_lock<std::mutex> jobsLock(jobsFinishedMtx);
            while (jobsFinished != HASH_RANGE_1) {
                condJobsFinished.wait(jobsLock);
            }
            jobsFinished = 0;
            condJobsFinished.notify_one();
            jobsLock.unlock();
            
            // 收集并汇总部分校验和
            uint64_t allCheckSums = 0;
            for (unsigned j = 0; j < HASH_RANGE_1; ++j) {
                allCheckSums += js->checkSumArray[j];
            }
            printCheckSum(allCheckSums, isLast);
        }
    }
}

/*==============================================================================
 * 检查函数
 *============================================================================*/

/**
 * @brief 检查向量是否在中间结果中
 * @return 1 如果向量不为 nullptr，0 否则
 */
unsigned isInInter(Vector* vector) {
    return vector != nullptr ? 1 : 0;
}

/*==============================================================================
 * 辅助函数
 *============================================================================*/

/**
 * @brief 获取关系所在的向量位置
 * 
 * 遍历所有映射数组，查找包含该关系的向量位置
 * 如果没有找到，返回第一个可用位置
 */
unsigned getVectorPos(InterMetaData* inter, unsigned relId) {
    for (unsigned i = 0; i < inter->maxNumOfVectors; ++i) {
        if (inter->mapRels[i]) {
            if (inter->mapRels[i][relId] != static_cast<unsigned>(-1)) {
                return i;
            }
        }
    }
    return getFirstAvailablePos(inter);
}

/**
 * @brief 获取第一个可用的向量位置
 * @return 第一个 mapRels[i] 为 nullptr 的位置索引
 */
unsigned getFirstAvailablePos(InterMetaData* inter) {
    for (unsigned i = 0; i < inter->maxNumOfVectors; ++i) {
        if (!inter->mapRels[i]) {
            return i;
        }
    }
    return 0;  // 默认返回0（理论上不会到达这里）
}

/**
 * @brief 创建映射数组
 * 
 * 分配并初始化映射数组，将 values 数组的内容复制到新分配的数组中
 */
void createMap(unsigned** mapRels, unsigned size, unsigned* values) {
    // -1 unsigned = 4294967295（假设不会有这么多关系）
    *mapRels = new unsigned[size];
    for (unsigned j = 0; j < size; ++j) {
        (*mapRels)[j] = values[j];
    }
}

/**
 * @brief 打印校验和
 * 
 * 将校验和输出到标准输出，如果不是最后一个校验和则输出空格
 */
void printCheckSum(uint64_t checkSum, unsigned isLast) {
    if (checkSum) {
        printf("%lu", checkSum);
    } else {
        printf("NULL");
    }
    
    if (isLast) {
        printf("\n");
        fflush(stdout);
    } else {
        printf(" ");
    }
}

/*==============================================================================
 * 销毁器
 *============================================================================*/

/**
 * @brief 销毁中间结果元数据
 * 
 * 释放所有分配的内存：
 * - 销毁所有向量
 * - 释放 interResults 和 mapRels 数组
 * - 释放主结构
 */
void destroyInterMetaData(InterMetaData* inter) {
    if (!inter) return;
    
    for (unsigned i = 0; i < inter->maxNumOfVectors; ++i) {
        // 销毁每个分区的向量
        for (unsigned v = 0; v < HASH_RANGE_1; ++v) {
            destroyVector(&inter->interResults[i][v]);
        }
        // 释放 interResults[i] 数组
        delete[] inter->interResults[i];
        // 释放 mapRels[i] 数组
        delete[] inter->mapRels[i];
    }
    
    // 释放主数组
    delete[] inter->interResults;
    delete[] inter->mapRels;
    
    // 释放主结构
    delete inter;
}

/**
 * @brief 销毁 RadixHashJoinInfo 结构
 * 
 * 释放 ColumnInfo、直方图、前缀和、索引数组等内存
 */
void destroyRadixHashJoinInfo(RadixHashJoinInfo* info) {
    if (!info) return;
    
    destroyColumnInfo(&info->unsorted);
    destroyColumnInfo(&info->sorted);
    delete[] info->hist;
    delete[] info->pSum;
    
    // 释放索引数组
    if (info->indexArray) {
        for (unsigned i = 0; i < HASH_RANGE_1; ++i) {
            if (info->indexArray[i]) {
                delete[] info->indexArray[i]->chainArray;
                delete[] info->indexArray[i]->bucketArray;
                delete info->indexArray[i];
            }
        }
        delete[] info->indexArray;
    }
}

/**
 * @brief 销毁 ColumnInfo 结构
 * 
 * 释放 values、rowIds 数组和 tuples 向量
 */
void destroyColumnInfo(ColumnInfo** c) {
    if (!c || !*c) return;
    
    delete[] (*c)->values;
    delete[] (*c)->rowIds;
    destroyVector(&(*c)->tuples);
    
    delete *c;
    *c = nullptr;
}

} // namespace radix_join
