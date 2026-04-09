#include "Build.hpp"

#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <condition_variable>

#include "JobScheduler.hpp"
#include "Queue.hpp"
#include "Utils.hpp"
#include "Joiner.hpp"

namespace radix_join {

/**
 * @brief 构建阶段主函数
 * 
 * 这是 Radix Hash Join 的第二阶段：构建索引
 * 
 * 详细步骤：
 * 1. 比较两个关系的元组数量，确定较小的关系（small）
 *    - small 关系用于构建索引
 *    - big 关系用于探测
 * 
 * 2. 为 small 关系初始化索引数组
 *    - 为每个非空 bucket 分配 Index 结构
 *    - 分配 chainArray 和 bucketArray
 * 
 * 3. 为每个 bucket 提交构建任务到线程池
 *    - 每个任务负责为一个 bucket 构建索引
 * 
 * 4. 等待所有任务完成
 *    - 使用 jobsFinished 计数器和条件变量同步
 * 
 * @param infoLeft 左关系信息
 * @param infoRight 右关系信息
 */
void build(RadixHashJoinInfo* infoLeft, RadixHashJoinInfo* infoRight) {
    // 确定 small 和 big 关系
    RadixHashJoinInfo* big;
    RadixHashJoinInfo* small;
    
    if (infoLeft->numOfTuples >= infoRight->numOfTuples) {
        big = infoLeft;
        small = infoRight;
    } else {
        big = infoRight;
        small = infoLeft;
    }
    
    big->isSmall = 0;
    small->isSmall = 1;
    
    // 初始化索引数组
    initializeIndexArray(small);
    
    // 重置任务完成计数器
    jobsFinished = 0;
    
    // 为每个 bucket 提交构建任务
    for (unsigned i = 0; i < HASH_RANGE_1; ++i) {
        BuildArg* arg = static_cast<BuildArg*>(js->buildJobs[i].argument);
        arg->bucket = i;
        arg->info = small;
        
        std::lock_guard<std::mutex> lock(queueMtx);
        enQueue(jobQueue, &js->buildJobs[i]);
        condNonEmpty.notify_one();
    }
    
    // 等待所有任务完成
    std::unique_lock<std::mutex> lock(jobsFinishedMtx);
    while (jobsFinished != HASH_RANGE_1) {
        condJobsFinished.wait(lock);
    }
    jobsFinished = 0;
    condJobsFinished.notify_one();
}

/**
 * @brief 初始化索引数组
 * 
 * 为每个非空 bucket 分配索引结构：
 * 1. 分配 indexArray 数组（HASH_RANGE_1 个指针）
 * 2. 对于每个非空 bucket：
 *    - 分配 Index 结构
 *    - 分配 chainArray（大小为 bucketSize）
 *    - 分配 bucketArray（大小为 HASH_RANGE_2）
 *    - 初始化为 0
 * 
 * 注意：空 bucket 的索引设为 nullptr
 * 
 * @param info 需要构建索引的关系信息
 */
void initializeIndexArray(RadixHashJoinInfo* info) {
    // 分配 indexArray
    info->indexArray = new Index*[HASH_RANGE_1];
    mallocCheck(info->indexArray, __FILE__, __LINE__);
    
    // 为每个 bucket 分配索引
    for (unsigned i = 0; i < HASH_RANGE_1; ++i) {
        // 空 bucket，索引设为 nullptr
        if (info->hist[i] == 0) {
            info->indexArray[i] = nullptr;
        } else {
            unsigned bucketSize = info->hist[i];
            
            // 分配 Index 结构
            info->indexArray[i] = new Index();
            mallocCheck(info->indexArray[i], __FILE__, __LINE__);
            
            // 分配 chainArray 和 bucketArray
            info->indexArray[i]->chainArray = new unsigned[bucketSize]();
            mallocCheck(info->indexArray[i]->chainArray, __FILE__, __LINE__);
            
            info->indexArray[i]->bucketArray = new unsigned[HASH_RANGE_2]();
            mallocCheck(info->indexArray[i]->bucketArray, __FILE__, __LINE__);
        }
    }
}

/**
 * @brief 构建索引线程函数
 * 
 * 为指定的 bucket 构建哈希索引：
 * 
 * 算法步骤：
 * 1. 获取 bucket 的起始位置和大小
 *    - start = pSum[bucket]
 *    - bucketSize = hist[bucket]
 * 
 * 2. 从 bucket 底部向上扫描（从后往前）
 *    - 这样可以保证链表中元素的顺序
 * 
 * 3. 对每个元素：
 *    - 计算第二级哈希值（HASH_FUN_2）
 *    - 如果 bucketArray[hash] 为空，直接存储位置
 *    - 否则，遍历 chainArray 找到空位存储
 * 
 * 4. 更新任务完成计数器
 * 
 * 注意：bucketArray 和 chainArray 中存储的是 1-based 位置
 *       0 表示空/结束
 * 
 * @param arg 指向 BuildArg 的指针
 */
void buildFunc(void* arg) {
    BuildArg* myArg = static_cast<BuildArg*>(arg);
    
    // 只处理非空 bucket
    if (myArg->info->indexArray[myArg->bucket] != nullptr) {
        // 获取 bucket 的起始位置和大小
        int start = static_cast<int>(myArg->info->pSum[myArg->bucket]);
        unsigned bucketSize = myArg->info->hist[myArg->bucket];
        
        // 从 bucket 底部向上扫描
        for (int j = start + static_cast<int>(bucketSize) - 1; j >= start; --j) {
            // 计算第二级哈希值
            uint64_t hash = HASH_FUN_2(myArg->info->sorted->values[j]);
            
            // 获取 bucketArray 中的值
            unsigned bucketValue = myArg->info->indexArray[myArg->bucket]->bucketArray[hash];
            
            if (bucketValue == 0) {
                // bucketArray 为空，直接存储位置（1-based）
                myArg->info->indexArray[myArg->bucket]->bucketArray[hash] = 
                    static_cast<unsigned>(j - start) + 1;
            } else {
                // 遍历 chainArray 找到空位
                unsigned chainPos = bucketValue - 1;
                traverseChain(chainPos, 
                             myArg->info->indexArray[myArg->bucket]->chainArray,
                             static_cast<unsigned>(j - start) + 1);
            }
        }
    }
    
    // 更新任务完成计数器
    std::lock_guard<std::mutex> lock(jobsFinishedMtx);
    ++jobsFinished;
    condJobsFinished.notify_one();
}

/**
 * @brief 遍历冲突链
 * 
 * 在 chainArray 中找到第一个空位（值为0的位置），
 * 将 posToBeStored 存储到该位置。
 * 
 * 如果当前位置非空，则沿着链继续查找（chainArray[chainPos] - 1）
 * 
 * @param chainPos 当前链位置
 * @param chainArray 链数组
 * @param posToBeStored 要存储的位置值（1-based）
 */
void traverseChain(unsigned chainPos, unsigned* chainArray, unsigned posToBeStored) {
    while (true) {
        // 找到空位
        if (chainArray[chainPos] == 0) {
            chainArray[chainPos] = posToBeStored;
            break;
        } else {
            // 继续遍历链
            chainPos = chainArray[chainPos] - 1;
        }
    }
}

} // namespace radix_join
