#include "Partition.hpp"

#include <cstdlib>
#include <cstring>
#include <mutex>

#include "JobScheduler.hpp"
#include "Queue.hpp"
#include "Utils.hpp"
#include "Vector.hpp"
#include "Joiner.hpp"

namespace radix_join {

/**
 * @brief 简单的barrier实现
 * 用于替代C++20的std::barrier
 * 
 * 使用代计数器确保barrier可以安全重用，避免竞态条件
 */
void barrier_wait() {
    std::unique_lock<std::mutex> lock(barrierMtx);
    unsigned generation = barrierGeneration;
    barrierCount++;
    
    if (barrierCount == barrierTotal) {
        // 所有线程都到达了，增加代计数器并通知所有线程
        barrierGeneration++;
        barrierCount = 0;
        barrierCond.notify_all();
    } else {
        // 等待其他线程到达（检查代计数器是否变化）
        barrierCond.wait(lock, [generation]() { return barrierGeneration != generation; });
    }
}

/**
 * @brief 直方图计算线程函数
 * 
 * 工作流程：
 * 1. 初始化直方图数组为0
 * 2. 遍历指定范围的数据，计算每个值的哈希值
 * 3. 在直方图中对应位置计数+1
 * 4. 通过 barrier 与其他线程同步
 */
void histFunc(void* arg) {
    HistArg* myArg = static_cast<HistArg*>(arg);
    
    // 初始化直方图为0
    for (unsigned i = 0; i < HASH_RANGE_1; ++i) {
        myArg->histogram[i] = 0;
    }
    
    // 计算直方图：统计每个哈希值的出现次数
    for (unsigned i = myArg->start; i < myArg->end; ++i) {
        myArg->histogram[HASH_FUN_1(myArg->values[i])] += 1;
    }
    
    // 通过 barrier 同步所有线程
    barrier_wait();
}

/**
 * @brief 分区线程函数
 * 
 * 工作流程：
 * 1. 遍历指定范围的数据
 * 2. 计算每个元素的哈希值
 * 3. 使用互斥锁保护，从 pSumCopy 获取写入位置
 * 4. 将元素写入 sorted 数组的对应位置
 * 5. 更新 pSumCopy
 * 6. 通过 barrier 与其他线程同步
 * 
 * 注意：使用互斥锁保护 pSumCopy 的并发访问，
 * 因为多个线程可能同时写入同一个 bucket
 */
void partitionFunc(void* arg) {
    PartitionArg* myArg = static_cast<PartitionArg*>(arg);
    
    for (unsigned i = myArg->start; i < myArg->end; ++i) {
        // 计算哈希值
        unsigned h;
        if (myArg->info->isInInter) {
            h = HASH_FUN_1(myArg->info->unsorted->values[i]);
        } else {
            h = HASH_FUN_1(myArg->info->col[i]);
        }
        
        // 使用互斥锁保护 pSumCopy 的访问
        // 锁作用域：获取offset、递增pSumCopy、写入sorted数组
        // 确保所有对共享数据的访问都在锁保护下
        unsigned offset;
        {
            std::lock_guard<std::mutex> lock(partitionMtxArray[h]);
            // 读取和修改pSumCopy必须在同一把锁下完成
            offset = myArg->pSumCopy[h];
            myArg->pSumCopy[h]++;
            
            // 写入 sorted 数组（也在锁保护下，防止并发写入冲突）
            Vector* sortedTuples = myArg->info->sorted->tuples;
            Vector* unsortedTuples = myArg->info->unsorted->tuples;
            
            if (myArg->info->isInInter) {
                myArg->info->sorted->values[offset] = myArg->info->unsorted->values[i];
                myArg->info->sorted->rowIds[offset] = myArg->info->unsorted->rowIds[i];
                // 复制元组数据
                insertAtPos(sortedTuples, 
                           &unsortedTuples->table[i * myArg->info->tupleSize], 
                           offset);
            } else {
                myArg->info->sorted->values[offset] = myArg->info->col[i];
                myArg->info->sorted->rowIds[offset] = i;
                insertAtPos(sortedTuples, &i, offset);
            }
        } // 锁在这里自动释放
    }
    
    // 通过 barrier 同步所有线程
    barrier_wait();
}

/**
 * @brief 分区主函数
 * 
 * 这是 Radix Hash Join 的第一阶段，将数据根据哈希值分区
 * 
 * 详细步骤：
 * 1. 内存分配：
 *    - 分配 unsorted 和 sorted 的 ColumnInfo 结构
 *    - 分配 values、rowIds 数组
 *    - 创建 tuples Vector
 * 
 * 2. 数据准备：
 *    - 如果数据在中间结果中，调用 scanJoin 填充 unsorted
 *    - 否则直接使用 col 数组
 * 
 * 3. 直方图计算（PARALLEL_HISTOGRAM 控制是否并行）：
 *    - 统计每个哈希值对应的元素数量
 *    - 用于后续计算前缀和
 * 
 * 4. 前缀和计算：
 *    - pSum[i] 表示哈希值为 i 的元素在 sorted 中的起始位置
 *    - 通过累加直方图得到
 * 
 * 5. 数据分区（PARALLEL_PARTITION 控制是否并行）：
 *    - 根据哈希值将元素放入 sorted 的正确位置
 *    - 相同哈希值的元素在 sorted 中是连续的
 * 
 * @param info 包含输入数据和输出缓冲区的信息结构
 */
void partition(RadixHashJoinInfo* info) {
    // ==================== 1. 分配内存 ====================
    
    // 分配 unsorted 结构
    info->unsorted = new ColumnInfo();
    
    info->unsorted->values = new uint64_t[info->numOfTuples];
    mallocCheck(info->unsorted->values, __FILE__, __LINE__);
    
    info->unsorted->rowIds = new unsigned[info->numOfTuples];
    mallocCheck(info->unsorted->rowIds, __FILE__, __LINE__);
    
    // 分配 sorted 结构
    info->sorted = new ColumnInfo();
    
    info->sorted->values = new uint64_t[info->numOfTuples];
    mallocCheck(info->sorted->values, __FILE__, __LINE__);
    
    info->sorted->rowIds = new unsigned[info->numOfTuples];
    mallocCheck(info->sorted->rowIds, __FILE__, __LINE__);
    
    info->indexArray = nullptr;
    
    // 创建 Vector 用于存储元组
    createVectorFixedSize(&info->sorted->tuples, info->tupleSize, info->numOfTuples);
    createVector(&info->unsorted->tuples, info->tupleSize);
    
    // ==================== 2. 数据准备 ====================
    
    // 如果数据在中间结果中，从中间结果获取数据
    if (info->isInInter) {
        scanJoin(info);
    }
    
    // ==================== 3. 直方图计算 ====================
    
    info->hist = new unsigned[HASH_RANGE_1]();
    mallocCheck(info->hist, __FILE__, __LINE__);
    
#if PARALLEL_HISTOGRAM
    // 并行直方图计算
    unsigned chunkSize = info->numOfTuples / js->threadNum;
    unsigned lastEnd = 0;
    unsigned i;
    
#if ENABLE_LOAD_BALANCE
    // 负载均衡模式：均匀分配任务
    for (i = 0; i < js->threadNum - 1; ++i) {
        HistArg* arg = static_cast<HistArg*>(js->histJobs[i].argument);
        arg->start = i * chunkSize;
        arg->end = arg->start + chunkSize;
        arg->values = (info->isInInter) ? info->unsorted->values : info->col;
        
        std::lock_guard<std::mutex> lock(queueMtx);
        enQueue(jobQueue, &js->histJobs[i]);
        condNonEmpty.notify_one();
        lastEnd = arg->end;
    }
    
    // 处理剩余数据
    HistArg* arg = static_cast<HistArg*>(js->histJobs[i].argument);
    arg->start = lastEnd;
    arg->end = info->numOfTuples;
    arg->values = (info->isInInter) ? info->unsorted->values : info->col;
    
    {
        std::lock_guard<std::mutex> lock(queueMtx);
        enQueue(jobQueue, &js->histJobs[i]);
        condNonEmpty.notify_one();
    }
#else
    // 默认模式：简单分配任务
    for (i = 0; i < js->threadNum; ++i) {
        HistArg* arg = static_cast<HistArg*>(js->histJobs[i].argument);
        arg->start = i * chunkSize;
        arg->end = (i == js->threadNum - 1) ? info->numOfTuples : arg->start + chunkSize;
        arg->values = (info->isInInter) ? info->unsorted->values : info->col;
        
        std::lock_guard<std::mutex> lock(queueMtx);
        enQueue(jobQueue, &js->histJobs[i]);
        condNonEmpty.notify_one();
    }
#endif
    
    // 等待所有线程完成
    barrier_wait();
    
    // 合并各线程的直方图
    for (unsigned i = 0; i < HASH_RANGE_1; ++i) {
        info->hist[i] = 0;
    }
    
    for (unsigned t = 0; t < js->threadNum; ++t) {
        for (unsigned h = 0; h < HASH_RANGE_1; ++h) {
            info->hist[h] += js->histArray[t][h];
        }
    }
#else
    // 串行直方图计算
    for (unsigned i = 0; i < info->numOfTuples; ++i) {
        if (info->isInInter) {
            info->hist[HASH_FUN_1(info->unsorted->values[i])] += 1;
        } else {
            info->hist[HASH_FUN_1(info->col[i])] += 1;
        }
    }
#endif
    
    // ==================== 4. 计算前缀和 ====================
    
    unsigned sum = 0;
    info->pSum = new unsigned[HASH_RANGE_1];
    mallocCheck(info->pSum, __FILE__, __LINE__);
    
    for (unsigned i = 0; i < HASH_RANGE_1; ++i) {
        info->pSum[i] = 0;
    }
    
    for (unsigned i = 0; i < HASH_RANGE_1; ++i) {
        info->pSum[i] = sum;
        sum += info->hist[i];
    }
    
    // 创建 pSum 副本用于分区
    unsigned* pSumCopy = new unsigned[HASH_RANGE_1];
    mallocCheck(pSumCopy, __FILE__, __LINE__);
    
    for (unsigned i = 0; i < HASH_RANGE_1; ++i) {
        pSumCopy[i] = info->pSum[i];
    }
    
    // ==================== 5. 数据分区 ====================
    
#if PARALLEL_PARTITION
    // 并行分区
    lastEnd = 0;
    
#if ENABLE_LOAD_BALANCE
    // 负载均衡模式：均匀分配任务
    for (i = 0; i < js->threadNum - 1; ++i) {
        PartitionArg* arg = static_cast<PartitionArg*>(js->partitionJobs[i].argument);
        arg->start = i * chunkSize;
        arg->end = arg->start + chunkSize;
        arg->pSumCopy = pSumCopy;
        arg->info = info;
        
        std::lock_guard<std::mutex> lock(queueMtx);
        enQueue(jobQueue, &js->partitionJobs[i]);
        condNonEmpty.notify_one();
        lastEnd = arg->end;
    }
    
    // 处理剩余数据
    PartitionArg* pArg = static_cast<PartitionArg*>(js->partitionJobs[i].argument);
    pArg->start = lastEnd;
    pArg->end = info->numOfTuples;
    pArg->pSumCopy = pSumCopy;
    pArg->info = info;
    
    {
        std::lock_guard<std::mutex> lock(queueMtx);
        enQueue(jobQueue, &js->partitionJobs[i]);
        condNonEmpty.notify_one();
    }
#else
    // 默认模式：简单分配任务
    for (i = 0; i < js->threadNum; ++i) {
        PartitionArg* arg = static_cast<PartitionArg*>(js->partitionJobs[i].argument);
        arg->start = i * chunkSize;
        arg->end = (i == js->threadNum - 1) ? info->numOfTuples : arg->start + chunkSize;
        arg->pSumCopy = pSumCopy;
        arg->info = info;
        
        std::lock_guard<std::mutex> lock(queueMtx);
        enQueue(jobQueue, &js->partitionJobs[i]);
        condNonEmpty.notify_one();
    }
#endif
    
    // 等待所有线程完成
    barrier_wait();
#else
    // 串行分区
    for (unsigned i = 0; i < info->numOfTuples; ++i) {
        // 计算哈希值
        unsigned h;
        if (info->isInInter) {
            h = HASH_FUN_1(info->unsorted->values[i]);
        } else {
            h = HASH_FUN_1(info->col[i]);
        }
        
        // 获取写入位置
        unsigned offset = pSumCopy[h];
        pSumCopy[h]++;
        
        // 写入 sorted 数组
        Vector* sortedTuples = info->sorted->tuples;
        Vector* unsortedTuples = info->unsorted->tuples;
        
        if (info->isInInter) {
            info->sorted->values[offset] = info->unsorted->values[i];
            info->sorted->rowIds[offset] = info->unsorted->rowIds[i];
            insertAtPos(sortedTuples, 
                       &unsortedTuples->table[i * info->tupleSize], 
                       offset);
        } else {
            info->sorted->values[offset] = info->col[i];
            info->sorted->rowIds[offset] = i;
            insertAtPos(sortedTuples, &i, offset);
        }
    }
#endif
    
    // 释放 pSumCopy
    delete[] pSumCopy;
}

} // namespace radix_join
