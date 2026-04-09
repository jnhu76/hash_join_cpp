#include "Probe.hpp"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <condition_variable>

#include "Build.hpp"
#include "Utils.hpp"
#include "Queue.hpp"
#include "JobScheduler.hpp"
#include "Joiner.hpp"

namespace radix_join {

/**
 * @brief Join 线程函数
 * 
 * 这是 Radix Hash Join 探测阶段的核心函数。
 * 每个线程负责探测一个 bucket，查找匹配项。
 * 
 * 详细步骤：
 * 1. 确定 small 和 big 关系（根据 isSmall 标志）
 * 2. 确定当前线程负责的 bucket 范围
 * 3. 遍历 big 关系中该 bucket 的所有元组
 * 4. 对每个元组：
 *    - 获取搜索值（searchValue）
 *    - 如果 small 关系的对应 bucket 为空，跳过
 *    - 计算第二级哈希值（HASH_FUN_2）
 *    - 在索引中查找匹配（通过 bucketArray 和 chainArray）
 *    - 对每个可能的匹配，调用 checkEqual 检查实际值
 * 5. 更新任务完成计数器
 * 
 * @param arg 指向 JoinArg 的指针
 */
void joinFunc(void* arg) {
    JoinArg* myArg = static_cast<JoinArg*>(arg);
    
    RadixHashJoinInfo* left = myArg->left;
    RadixHashJoinInfo* right = myArg->right;
    Vector* results = myArg->results;
    unsigned searchBucket = myArg->bucket;
    
    // 确定 small 和 big 关系
    RadixHashJoinInfo* big = left->isSmall ? right : left;
    RadixHashJoinInfo* small = left->isSmall ? left : right;
    
    // 确定当前线程负责的 bucket 范围
    unsigned tStart = big->pSum[searchBucket];
    unsigned tEnd = tStart + big->hist[searchBucket];
    
    // 分配元组缓冲区
    unsigned tupleSize = small->tupleSize + big->tupleSize;
    unsigned* tupleToInsert = new unsigned[tupleSize];
    mallocCheck(tupleToInsert, __FILE__, __LINE__);
    
    // 遍历 big 关系中该 bucket 的所有元组
    for (unsigned i = tStart; i < tEnd; ++i) {
        // 获取搜索值
        unsigned searchValue = static_cast<unsigned>(big->sorted->values[i]);
        
        // 如果 small 关系的对应 bucket 为空，跳过
        if (small->hist[searchBucket] == 0) {
            continue;
        }
        
        // 获取 small 关系 bucket 的起始位置
        unsigned start = small->pSum[searchBucket];
        
        // 获取该 bucket 的索引
        Index* searchIndex = small->indexArray[searchBucket];
        
        // 防御性检查：确保 searchIndex 和 bucketArray 不为空
        if (!searchIndex || !searchIndex->bucketArray) {
            continue;
        }
        
        // 计算第二级哈希值
        uint64_t hash = HASH_FUN_2(searchValue);
        
        // 如果 bucketArray 中对应位置为空，说明没有匹配
        if (searchIndex->bucketArray[hash] == 0) {
            continue;
        }
        
        // 从 bucketArray 获取链的起始位置
        int k = static_cast<int>(searchIndex->bucketArray[hash]) - 1;
        
        // 检查第一个匹配
        checkEqual(small, big, i, start, searchValue, 
                   static_cast<unsigned>(k), results, tupleToInsert);
        
        // 遍历冲突链，检查所有可能的匹配
        while (true) {
            // 链结束
            if (searchIndex->chainArray[k] == 0) {
                break;
            }
            
            // 继续遍历链
            k = static_cast<int>(searchIndex->chainArray[k]) - 1;
            checkEqual(small, big, i, start, searchValue, 
                       static_cast<unsigned>(k), results, tupleToInsert);
        }
    }
    
    // 释放元组缓冲区
    delete[] tupleToInsert;
    
    // 更新任务完成计数器
    std::lock_guard<std::mutex> lock(jobsFinishedMtx);
    ++jobsFinished;
    condJobsFinished.notify_one();
}

/**
 * @brief 检查相等性并构造结果元组
 * 
 * 检查 small 关系和 big 关系在指定位置的值是否相等。
 * 如果相等，则构造结果元组并插入结果向量。
 * 
 * 注意：actualRow = start + pseudoRow，将 bucket 内的相对位置
 *       转换为 sorted 数组中的绝对位置。
 * 
 * @param small 较小的关系（已构建索引）
 * @param big 较大的关系（未构建索引）
 * @param i big 关系的当前行
 * @param start small 关系 bucket 的起始位置
 * @param searchValue 要搜索的值
 * @param pseudoRow bucket 内的相对行号
 * @param results 结果向量
 * @param tupleToInsert 用于存储构造的元组
 */
void checkEqual(RadixHashJoinInfo* small, RadixHashJoinInfo* big, 
                unsigned i, unsigned start, unsigned searchValue, 
                unsigned pseudoRow, Vector* results, unsigned* tupleToInsert) {
    // 计算实际行号
    unsigned actualRow = start + pseudoRow;
    
    // 防御性检查：确保 actualRow 在有效范围内
    // sorted->values 数组大小为 numOfTuples
    if (!small->sorted || !small->sorted->values || actualRow >= small->numOfTuples) {
        return;
    }
    
    // 检查值是否相等
    if (small->sorted->values[actualRow] == searchValue) {
        // 构造结果元组
        constructTuple(small, big, actualRow, i, tupleToInsert);
        // 插入结果向量
        insertAtVector(results, tupleToInsert);
    }
}

/**
 * @brief 构造结果元组
 * 
 * 根据 small 和 big 关系的匹配行构造结果元组。
 * 元组格式：[left tuple][right tuple]
 * 
 * 注意：需要根据 isLeft 标志确定哪个关系在左，哪个在右。
 *       同时需要根据 isSmall 标志确定从哪个数组获取元组数据。
 * 
 * @param small 较小的关系
 * @param big 较大的关系
 * @param actualRow small 关系的实际行号
 * @param i big 关系的当前行
 * @param target 输出元组缓冲区
 */
void constructTuple(RadixHashJoinInfo* small, RadixHashJoinInfo* big, 
                    unsigned actualRow, unsigned i, unsigned* target) {
    unsigned k = 0;
    
    // 确定 left 和 right 关系
    RadixHashJoinInfo* left = small->isLeft ? small : big;
    RadixHashJoinInfo* right = small->isLeft ? big : small;
    
    // 添加 left 关系的元组数据
    unsigned* t;
    Vector* leftTuples = left->sorted->tuples;
    Vector* rightTuples = right->sorted->tuples;
    
    if (left->isSmall) {
        t = &leftTuples->table[actualRow * left->tupleSize];
    } else {
        t = &leftTuples->table[i * left->tupleSize];
    }
    
    for (unsigned idx = 0; idx < left->tupleSize; ++idx) {
        target[k++] = t[idx];
    }
    
    // 添加 right 关系的元组数据
    if (right->isSmall) {
        t = &rightTuples->table[actualRow * right->tupleSize];
    } else {
        t = &rightTuples->table[i * right->tupleSize];
    }
    
    for (unsigned idx = 0; idx < right->tupleSize; ++idx) {
        target[k++] = t[idx];
    }
}

/**
 * @brief 探测阶段主函数
 * 
 * 这是 Radix Hash Join 的第三阶段。
 * 
 * 详细步骤：
 * 1. 重置任务完成计数器
 * 2. 为每个 bucket 提交 join 任务
 * 3. 等待所有任务完成
 * 
 * @param left 左关系信息
 * @param right 右关系信息
 * @param results 结果向量
 */
void probe(RadixHashJoinInfo* left, RadixHashJoinInfo* right, Vector* results) {
    // 重置任务完成计数器
    jobsFinished = 0;
    
    // 为每个 bucket 提交 join 任务
    for (unsigned i = 0; i < HASH_RANGE_1; ++i) {
        JoinArg* arg = static_cast<JoinArg*>(js->joinJobs[i].argument);
        arg->bucket = i;
        arg->left = left;
        arg->right = right;
        arg->results = results;
        
        std::lock_guard<std::mutex> lock(queueMtx);
        enQueue(jobQueue, &js->joinJobs[i]);
        condNonEmpty.notify_one();
    }
    
    // 等待所有任务完成
    std::unique_lock<std::mutex> lock(jobsFinishedMtx);
    while (jobsFinished != HASH_RANGE_1) {
        condJobsFinished.wait(lock);
    }
    jobsFinished = 0;
}

} // namespace radix_join
