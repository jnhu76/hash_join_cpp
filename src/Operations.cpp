/*==============================================================================
 * Operations.cpp - 操作算子模块
 * 
 * 该模块实现了查询执行过程中的各种操作算子，包括：
 * - 列等值过滤（同一关系内的列比较）
 * - 过滤操作（与常量比较）
 * - 四种 join 策略的实现
 * 
 * 转换说明（C -> C++20）：
 * - malloc/free 替换为 new/delete
 * - pthread 互斥锁替换为 std::mutex/lock_guard
 * - 保持与 JobScheduler、Vector、Intermediate 的交互
 * - 所有线程函数保持 void(void*) 签名以兼容 Job 系统
 *============================================================================*/

#include <cstdio>
#include <mutex>
#include <condition_variable>

#include "Operations.hpp"
#include "Intermediate.hpp"
#include "Vector.hpp"
#include "Partition.hpp"
#include "Build.hpp"
#include "Probe.hpp"
#include "Queue.hpp"
#include "JobScheduler.hpp"
#include "Joiner.hpp"

namespace radix_join {

/*==============================================================================
 * 列等值操作函数
 *============================================================================*/

/**
 * @brief 对原始关系应用列等值过滤
 * 
 * 当关系不在中间结果中时，扫描原始关系的所有元组，
 * 保留满足 leftCol[i] == rightCol[i] 的行。
 * 
 * 算法：
 * 1. 为每个分区创建结果向量
 * 2. 扫描所有元组，将满足条件的行ID插入到0号向量
 * （单线程执行，因为数据量通常不大）
 */
void colEquality(uint64_t* leftCol, uint64_t* rightCol, unsigned numOfTuples, Vector** vector) {
    // 为每个分区创建结果向量
    for (unsigned i = 0; i < HASH_RANGE_1; ++i) {
        createVector(vector + i, 1);
    }
    
    // 扫描所有元组，将满足条件的行ID插入到0号向量
    // 注意：这里只使用0号向量，其他向量保持为空
    for (unsigned i = 0; i < numOfTuples; ++i) {
        if (leftCol[i] == rightCol[i]) {
            insertAtVector(*vector, &i);
        }
    }
}

/**
 * @brief 对中间结果应用列等值过滤
 * 
 * 当关系已在中间结果中时，使用多线程并行扫描中间结果中的元组，
 * 保留满足 leftCol[rowIdLeft] == rightCol[rowIdRight] 的元组。
 * 
 * 算法：
 * 1. 创建新的结果向量数组
 * 2. 提交并行任务到 JobScheduler
 * 3. 等待所有任务完成
 * 4. 销毁旧向量，替换为新向量
 */
void colEqualityInter(uint64_t* leftCol, uint64_t* rightCol, unsigned posLeft, unsigned posRight, Vector** vector) {
    jobsFinished = 0;
    
    // 创建新的结果向量数组
    Vector** results = new Vector*[HASH_RANGE_1];
    for (unsigned i = 0; i < HASH_RANGE_1; ++i) {
        createVector(results + i, getTupleSize(vector[0]));
    }
    
    // 提交并行任务到 JobScheduler
    for (unsigned i = 0; i < HASH_RANGE_1; ++i) {
        ColEqualityArg* arg = static_cast<ColEqualityArg*>(js->colEqualityJobs[i].argument);
        arg->newVector = results[i];
        arg->oldVector = vector[i];
        arg->leftCol = leftCol;
        arg->rightCol = rightCol;
        arg->posLeft = posLeft;
        arg->posRight = posRight;
        
        std::unique_lock<std::mutex> lock(queueMtx);
        [[maybe_unused]] bool enqueueResult = jobQueue->enQueue(&js->colEqualityJobs[i]);
        condNonEmpty.notify_one();
    }
    
    // 等待所有任务完成
    std::unique_lock<std::mutex> jobsLock(jobsFinishedMtx);
    while (jobsFinished != HASH_RANGE_1) {
        condJobsFinished.wait(jobsLock);
    }
    jobsFinished = 0;
    condJobsFinished.notify_one();
    jobsLock.unlock();
    
    // 销毁旧向量
    for (unsigned i = 0; i < HASH_RANGE_1; ++i) {
        destroyVector(vector + i);
    }
    
    // 替换为新向量
    for (unsigned i = 0; i < HASH_RANGE_1; ++i) {
        vector[i] = results[i];
    }
    
    delete[] results;
}

/*==============================================================================
 * 过滤操作函数
 *============================================================================*/

/**
 * @brief 过滤线程函数
 * 
 * 工作线程执行的过滤任务：
 * 1. 创建输出向量
 * 2. 扫描指定范围的列数据
 * 3. 将满足 compare(col[i], cmp, constant) 的行ID插入到结果向量
 * 4. 增加 jobsFinished 计数并通知主线程
 */
void filterFunc(void* arg) {
    FilterArg* myarg = static_cast<FilterArg*>(arg);
    
    // 创建输出向量
    createVector(myarg->vector, 1);
    
    // 扫描指定范围，将满足条件的行ID插入到结果向量
    for (unsigned i = myarg->start; i < myarg->end; ++i) {
        if (compare(myarg->col[i], myarg->cmp, myarg->constant)) {
            insertAtVector(*myarg->vector, &i);
        }
    }
    
    // 通知主线程任务完成
    std::unique_lock<std::mutex> lock(jobsFinishedMtx);
    ++jobsFinished;
    condJobsFinished.notify_one();
}

/**
 * @brief 对中间结果应用过滤条件
 * 
 * 扫描中间结果中的元组，保留满足 compare(col[rowId], cmp, constant) 的元组。
 * 
 * 算法：
 * 1. 保存旧向量
 * 2. 创建新向量
 * 3. 调用 scanFilter 进行过滤
 * 4. 销毁旧向量
 */
void filterInter(uint64_t* col, Comparison cmp, uint64_t constant, Vector** vector) {
    // 保存旧向量
    Vector* old = *vector;
    
    // 创建新向量
    createVector(vector, 1);
    
    // 扫描旧向量，将满足条件的元组插入到新向量
    scanFilter(*vector, old, col, cmp, constant);
    
    // 销毁旧向量
    destroyVector(&old);
}

/*==============================================================================
 * Join 操作函数 - 四种策略
 *============================================================================*/

/**
 * @brief 非中间结果与非中间结果的连接
 * 
 * 两个关系都不在中间结果中（首次连接）。
 * 执行完整的分区-构建-探测流程，创建新的中间结果。
 * 
 * 算法：
 * 1. 对两列进行分区
 * 2. 为较小的关系构建索引
 * 3. 提交并行探测任务
 * 4. 等待所有任务完成
 * 5. 更新 mapRels 和 interResults
 * 6. 清理旧数据
 */
void joinNonInterNonInter(InterMetaData* inter, RadixHashJoinInfo* left, RadixHashJoinInfo* right) {
    // 对两列进行分区
    partition(left);
    partition(right);
    
    // 为较小的关系构建索引
    build(left, right);
    left->isLeft = 1;
    right->isLeft = 0;
    
    // 提交并行探测任务
    jobsFinished = 0;
    Vector** results = new Vector*[HASH_RANGE_1];
    for (unsigned i = 0; i < HASH_RANGE_1; ++i) {
        createVector(results + i, left->tupleSize + right->tupleSize);
    }
    
    for (unsigned i = 0; i < HASH_RANGE_1; ++i) {
        JoinArg* arg = static_cast<JoinArg*>(js->joinJobs[i].argument);
        arg->bucket = i;
        arg->left = left;
        arg->right = right;
        arg->results = results[i];
        
        std::unique_lock<std::mutex> lock(queueMtx);
        [[maybe_unused]] bool enqueueResult = jobQueue->enQueue(&js->joinJobs[i]);
        condNonEmpty.notify_one();
    }
    
    // 等待所有任务完成
    std::unique_lock<std::mutex> jobsLock(jobsFinishedMtx);
    while (jobsFinished != HASH_RANGE_1) {
        condJobsFinished.wait(jobsLock);
    }
    jobsFinished = 0;
    condJobsFinished.notify_one();
    jobsLock.unlock();
    
    // 更新 mapRels 和 interResults
    // 构造新的映射数组
    unsigned* newMap = new unsigned[inter->queryRelations];
    for (unsigned i = 0; i < inter->queryRelations; ++i) {
        newMap[i] = static_cast<unsigned>(-1);
    }
    
    newMap[left->relId] = 0;
    newMap[right->relId] = 1;
    
    // 释放旧的映射数组，销毁旧的向量
    if (*left->ptrToMap) {
        delete[] *left->ptrToMap;
        *left->ptrToMap = nullptr;
    }
    if (*right->ptrToMap) {
        delete[] *right->ptrToMap;
        *right->ptrToMap = nullptr;
    }
    
    for (unsigned i = 0; i < HASH_RANGE_1; ++i) {
        destroyVector(left->ptrToVec + i);
        destroyVector(right->ptrToVec + i);
    }
    
    // 将新的映射和结果附加到第一个可用位置
    unsigned pos = getFirstAvailablePos(inter);
    inter->mapRels[pos] = newMap;
    for (unsigned i = 0; i < HASH_RANGE_1; ++i) {
        inter->interResults[pos][i] = results[i];
    }
    
    delete[] results;
    destroyRadixHashJoinInfo(left);
    destroyRadixHashJoinInfo(right);
}

/**
 * @brief 非中间结果与中间结果的连接
 * 
 * 左关系不在中间结果中，右关系在中间结果中。
 * 将新关系与已有中间结果连接。
 * 
 * 算法与 joinNonInterNonInter 类似，但映射数组的构造不同：
 * - 新关系的 rowId 放在位置 0
 * - 中间结果中的关系的 rowId 位置向后偏移 1
 */
void joinNonInterInter(InterMetaData* inter, RadixHashJoinInfo* left, RadixHashJoinInfo* right) {
    // 对两列进行分区
    partition(left);
    partition(right);
    
    // 为较小的关系构建索引
    build(left, right);
    left->isLeft = 1;
    right->isLeft = 0;
    
    // 提交并行探测任务
    jobsFinished = 0;
    Vector** results = new Vector*[HASH_RANGE_1];
    for (unsigned i = 0; i < HASH_RANGE_1; ++i) {
        createVector(results + i, left->tupleSize + right->tupleSize);
    }
    
    for (unsigned i = 0; i < HASH_RANGE_1; ++i) {
        JoinArg* arg = static_cast<JoinArg*>(js->joinJobs[i].argument);
        arg->bucket = i;
        arg->left = left;
        arg->right = right;
        arg->results = results[i];
        
        std::unique_lock<std::mutex> lock(queueMtx);
        [[maybe_unused]] bool enqueueResult = jobQueue->enQueue(&js->joinJobs[i]);
        condNonEmpty.notify_one();
    }
    
    // 等待所有任务完成
    std::unique_lock<std::mutex> jobsLock(jobsFinishedMtx);
    while (jobsFinished != HASH_RANGE_1) {
        condJobsFinished.wait(jobsLock);
    }
    jobsFinished = 0;
    condJobsFinished.notify_one();
    jobsLock.unlock();
    
    // 更新 mapRels 和 interResults
    // 构造新的映射数组
    unsigned* newMap = new unsigned[inter->queryRelations];
    
    newMap[left->relId] = 0;
    for (unsigned i = 0; i < inter->queryRelations; ++i) {
        if (i != left->relId) {
            newMap[i] = (right->map[i] != static_cast<unsigned>(-1)) ? 1 + right->map[i] : static_cast<unsigned>(-1);
        }
    }
    
    // 释放旧的映射数组，销毁旧的向量
    if (*left->ptrToMap) {
        delete[] *left->ptrToMap;
        *left->ptrToMap = nullptr;
    }
    if (*right->ptrToMap) {
        delete[] *right->ptrToMap;
        *right->ptrToMap = nullptr;
    }
    
    for (unsigned i = 0; i < HASH_RANGE_1; ++i) {
        destroyVector(left->ptrToVec + i);
        destroyVector(right->ptrToVec + i);
    }
    
    // 将新的映射和结果附加到第一个可用位置
    unsigned pos = getFirstAvailablePos(inter);
    inter->mapRels[pos] = newMap;
    for (unsigned i = 0; i < HASH_RANGE_1; ++i) {
        inter->interResults[pos][i] = results[i];
    }
    
    delete[] results;
    destroyRadixHashJoinInfo(left);
    destroyRadixHashJoinInfo(right);
}

/**
 * @brief 中间结果与非中间结果的连接
 * 
 * 左关系在中间结果中，右关系不在中间结果中。
 * 将已有中间结果与新关系连接。
 * 
 * 算法与 joinNonInterNonInter 类似，但映射数组的构造不同：
 * - 保持中间结果中各关系的 rowId 位置不变
 * - 新关系的 rowId 放在 left->tupleSize 位置
 */
void joinInterNonInter(InterMetaData* inter, RadixHashJoinInfo* left, RadixHashJoinInfo* right) {
    // 对两列进行分区
    partition(left);
    partition(right);
    
    // 为较小的关系构建索引
    build(left, right);
    left->isLeft = 1;
    right->isLeft = 0;
    
    // 提交并行探测任务
    jobsFinished = 0;
    Vector** results = new Vector*[HASH_RANGE_1];
    for (unsigned i = 0; i < HASH_RANGE_1; ++i) {
        createVector(results + i, left->tupleSize + right->tupleSize);
    }
    
    for (unsigned i = 0; i < HASH_RANGE_1; ++i) {
        JoinArg* arg = static_cast<JoinArg*>(js->joinJobs[i].argument);
        arg->bucket = i;
        arg->left = left;
        arg->right = right;
        arg->results = results[i];
        
        std::unique_lock<std::mutex> lock(queueMtx);
        [[maybe_unused]] bool enqueueResult = jobQueue->enQueue(&js->joinJobs[i]);
        condNonEmpty.notify_one();
    }
    
    // 等待所有任务完成
    std::unique_lock<std::mutex> jobsLock(jobsFinishedMtx);
    while (jobsFinished != HASH_RANGE_1) {
        condJobsFinished.wait(jobsLock);
    }
    jobsFinished = 0;
    condJobsFinished.notify_one();
    jobsLock.unlock();
    
    // 更新 mapRels 和 interResults
    // 构造新的映射数组
    unsigned* newMap = new unsigned[inter->queryRelations];
    
    for (unsigned i = 0; i < inter->queryRelations; ++i) {
        newMap[i] = left->map[i];
    }
    newMap[right->relId] = left->tupleSize;
    
    // 释放旧的映射数组，销毁旧的向量
    if (*left->ptrToMap) {
        delete[] *left->ptrToMap;
        *left->ptrToMap = nullptr;
    }
    if (*right->ptrToMap) {
        delete[] *right->ptrToMap;
        *right->ptrToMap = nullptr;
    }
    
    for (unsigned i = 0; i < HASH_RANGE_1; ++i) {
        destroyVector(left->ptrToVec + i);
        destroyVector(right->ptrToVec + i);
    }
    
    // 将新的映射和结果附加到第一个可用位置
    unsigned pos = getFirstAvailablePos(inter);
    inter->mapRels[pos] = newMap;
    for (unsigned i = 0; i < HASH_RANGE_1; ++i) {
        inter->interResults[pos][i] = results[i];
    }
    
    delete[] results;
    destroyRadixHashJoinInfo(left);
    destroyRadixHashJoinInfo(right);
}

/**
 * @brief 中间结果与中间结果的连接
 * 
 * 两个关系都在中间结果中。
 * 
 * 特殊情况：如果两个关系在同一个中间结果向量中（left->vector == right->vector），
 * 则使用 colEqualityInter 进行列等值过滤。
 * 
 * 否则，执行完整的分区-构建-探测流程合并两个中间结果。
 * 映射数组的构造：
 * - 保持左中间结果中各关系的 rowId 位置不变
 * - 右中间结果中各关系的 rowId 位置向后偏移 left->tupleSize
 */
void joinInterInter(InterMetaData* inter, RadixHashJoinInfo* left, RadixHashJoinInfo* right) {
    // 特殊情况：两个关系在同一个中间结果向量中
    if (left->vector == right->vector) {
        unsigned posLeft = left->map[left->relId];
        unsigned posRight = right->map[right->relId];
        colEqualityInter(left->col, right->col, posLeft, posRight, left->ptrToVec);
        return;
    }
    
    // 对两列进行分区
    partition(left);
    partition(right);
    
    // 为较小的关系构建索引
    build(left, right);
    left->isLeft = 1;
    right->isLeft = 0;
    
    // 提交并行探测任务
    jobsFinished = 0;
    Vector** results = new Vector*[HASH_RANGE_1];
    for (unsigned i = 0; i < HASH_RANGE_1; ++i) {
        createVector(results + i, left->tupleSize + right->tupleSize);
    }
    
    for (unsigned i = 0; i < HASH_RANGE_1; ++i) {
        JoinArg* arg = static_cast<JoinArg*>(js->joinJobs[i].argument);
        arg->bucket = i;
        arg->left = left;
        arg->right = right;
        arg->results = results[i];
        
        std::unique_lock<std::mutex> lock(queueMtx);
        [[maybe_unused]] bool enqueueResult = jobQueue->enQueue(&js->joinJobs[i]);
        condNonEmpty.notify_one();
    }
    
    // 等待所有任务完成
    std::unique_lock<std::mutex> jobsLock(jobsFinishedMtx);
    while (jobsFinished != HASH_RANGE_1) {
        condJobsFinished.wait(jobsLock);
    }
    jobsFinished = 0;
    condJobsFinished.notify_one();
    jobsLock.unlock();
    
    // 更新 mapRels 和 interResults
    // 构造新的映射数组
    unsigned* newMap = new unsigned[inter->queryRelations];
    for (unsigned i = 0; i < inter->queryRelations; ++i) {
        newMap[i] = left->map[i];
    }
    
    for (unsigned i = 0; i < inter->queryRelations; ++i) {
        if (newMap[i] == static_cast<unsigned>(-1)) {
            newMap[i] = (right->map[i] != static_cast<unsigned>(-1)) ? right->map[i] + left->tupleSize : static_cast<unsigned>(-1);
        }
    }
    
    // 释放旧的映射数组，销毁旧的向量
    if (*left->ptrToMap) {
        delete[] *left->ptrToMap;
        *left->ptrToMap = nullptr;
    }
    if (*right->ptrToMap) {
        delete[] *right->ptrToMap;
        *right->ptrToMap = nullptr;
    }
    
    for (unsigned i = 0; i < HASH_RANGE_1; ++i) {
        destroyVector(left->ptrToVec + i);
        destroyVector(right->ptrToVec + i);
    }
    
    // 将新的映射和结果附加到第一个可用位置
    unsigned pos = getFirstAvailablePos(inter);
    inter->mapRels[pos] = newMap;
    for (unsigned i = 0; i < HASH_RANGE_1; ++i) {
        inter->interResults[pos][i] = results[i];
    }
    
    delete[] results;
    destroyRadixHashJoinInfo(left);
    destroyRadixHashJoinInfo(right);
}

} // namespace radix_join
