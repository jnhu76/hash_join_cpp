#include "JobScheduler.hpp"
#include "Queue.hpp"
#include "Utils.hpp"
#include "Joiner.hpp"
#include "Partition.hpp"
#include "Build.hpp"
#include "Probe.hpp"
#include "Vector.hpp"
#include "Operations.hpp"

#include <iostream>
#include <cstring>

namespace radix_join {

/*==============================================================================
 * 全局变量定义
 *============================================================================*/

// 队列互斥锁 - 保护工作队列的访问
std::mutex queueMtx;

// 任务完成计数互斥锁
std::mutex jobsFinishedMtx;

// 队列非空条件变量 - 通知工作线程有新任务
std::condition_variable condNonEmpty;

// 所有任务完成条件变量 - 通知主线程所有任务已完成
std::condition_variable condJobsFinished;

// 线程同步变量 - 用于 histogram/partition 阶段的线程同步
std::mutex barrierMtx;
std::condition_variable barrierCond;
unsigned barrierCount = 0;
unsigned barrierTotal = 0;
unsigned barrierGeneration = 0;  // 代计数器，确保barrier可安全重用

// 任务队列指针
Queue* jobQueue = nullptr;

// JobScheduler 全局指针
JobScheduler* js = nullptr;

// 分区互斥锁数组
std::mutex* partitionMtxArray = nullptr;

// 已完成任务计数
unsigned jobsFinished = 0;

// 外部函数声明 - 这些函数在其他模块中定义
// 注意：这些是线程工作函数，使用 void(*)(void*) 签名
void histFunc(void* arg);
void partitionFunc(void* arg);
void buildFunc(void* arg);
void joinFunc(void* arg);
void colEqualityFunc(void* arg);
void filterFunc(void* arg);
void checkSumFunc(void* arg);

/*==============================================================================
 * 工作线程函数
 *============================================================================*/

void threadFunc(void* arg) {
    (void)arg; // 未使用参数，避免编译器警告

    while (true) {
        // 获取互斥锁并检查队列中是否有任务
        std::unique_lock<std::mutex> lock(queueMtx);
        
        // 等待直到队列非空
        condNonEmpty.wait(lock, []() {
            return !jobQueue->isEmpty();
        });

        // 从队列中取出任务
        Job* job = static_cast<Job*>(jobQueue->deQueue());
        
        // 通知其他等待的线程（可能有空间了）
        condNonEmpty.notify_one();
        
        // 释放锁
        lock.unlock();

        // 特殊任务 nullptr 表示程序结束
        if (job == nullptr) {
            return; // 直接返回，线程结束
        }

        // 执行任务函数
        (*(job->function))(job->argument);
    }
}

/*==============================================================================
 * JobScheduler 创建和销毁
 *============================================================================*/

void createJobScheduler(JobScheduler** jsPtr) {
    // 创建 JobScheduler 实例
    *jsPtr = new JobScheduler();
    if (*jsPtr == nullptr) {
        throw std::runtime_error("[ERROR] Failed to allocate JobScheduler");
    }

    // 设置线程数量
    (*jsPtr)->threadNum = THREAD_NUM;

    // 创建任务队列，最大容量 1000
    jobQueue = new Queue(1000);
    if (jobQueue == nullptr) {
        throw std::runtime_error("[ERROR] Failed to create job queue");
    }

    // 创建工作线程
    for (unsigned i = 0; i < (*jsPtr)->threadNum; ++i) {
        (*jsPtr)->threads.emplace_back(threadFunc, nullptr);
    }

    // 初始化分区互斥锁数组
    // 注意：HASH_RANGE_1 在其他模块中定义，这里使用动态大小
    // 默认使用一个合理的初始值，后续可以根据实际值调整
    partitionMtxArray = new std::mutex[HASH_RANGE_1];

    // 初始化线程同步变量
    barrierCount = 0;
    barrierTotal = (*jsPtr)->threadNum + 1; // 工作线程数 + 主线程

    // 创建各种任务数组
    createJobArrays(*jsPtr);

    // 设置全局指针
    js = *jsPtr;
}

void createJobArrays(JobScheduler* js) {
    // 创建校验和数组
    js->checkSumArray.resize(HASH_RANGE_1);

    // 创建直方图数组 - 每个线程一个直方图
    js->histArray.resize(js->threadNum);
    for (unsigned i = 0; i < js->threadNum; ++i) {
        js->histArray[i] = new unsigned[HASH_RANGE_1]();
        if (js->histArray[i] == nullptr) {
            throw std::runtime_error("[ERROR] Failed to allocate histogram array");
        }
    }

    // 创建直方图任务数组
    js->histJobs = new Job[js->threadNum];
    if (js->histJobs == nullptr) {
        throw std::runtime_error("[ERROR] Failed to allocate histJobs");
    }
    for (unsigned i = 0; i < js->threadNum; ++i) {
        HistArg* arg = new HistArg();
        arg->histogram = js->histArray[i];
        js->histJobs[i] = Job(histFunc, arg);
    }

    // 创建分区任务数组
    js->partitionJobs = new Job[js->threadNum];
    if (js->partitionJobs == nullptr) {
        throw std::runtime_error("[ERROR] Failed to allocate partitionJobs");
    }
    for (unsigned i = 0; i < js->threadNum; ++i) {
        js->partitionJobs[i] = Job(partitionFunc, new PartitionArg());
    }

    // 创建构建索引任务数组
    js->buildJobs = new Job[HASH_RANGE_1];
    if (js->buildJobs == nullptr) {
        throw std::runtime_error("[ERROR] Failed to allocate buildJobs");
    }
    for (unsigned i = 0; i < HASH_RANGE_1; ++i) {
        js->buildJobs[i] = Job(buildFunc, new BuildArg());
    }

    // 创建连接任务数组
    js->joinJobs = new Job[HASH_RANGE_1];
    if (js->joinJobs == nullptr) {
        throw std::runtime_error("[ERROR] Failed to allocate joinJobs");
    }
    for (unsigned i = 0; i < HASH_RANGE_1; ++i) {
        js->joinJobs[i] = Job(joinFunc, new JoinArg());
    }

    // 创建列相等性检查任务数组
    js->colEqualityJobs = new Job[HASH_RANGE_1];
    if (js->colEqualityJobs == nullptr) {
        throw std::runtime_error("[ERROR] Failed to allocate colEqualityJobs");
    }
    for (unsigned i = 0; i < HASH_RANGE_1; ++i) {
        js->colEqualityJobs[i] = Job(colEqualityFunc, new ColEqualityArg());
    }

    // 创建过滤任务数组
    js->filterJobs = new Job[HASH_RANGE_1];
    if (js->filterJobs == nullptr) {
        throw std::runtime_error("[ERROR] Failed to allocate filterJobs");
    }
    for (unsigned i = 0; i < HASH_RANGE_1; ++i) {
        js->filterJobs[i] = Job(filterFunc, new FilterArg());
    }

    // 创建校验和计算任务数组
    js->checkSumJobs = new Job[HASH_RANGE_1];
    if (js->checkSumJobs == nullptr) {
        throw std::runtime_error("[ERROR] Failed to allocate checkSumJobs");
    }
    for (unsigned i = 0; i < HASH_RANGE_1; ++i) {
        js->checkSumJobs[i] = Job(checkSumFunc, new CheckSumArg());
    }
}

void destroyJobScheduler(JobScheduler* js) {
    if (js == nullptr) {
        return;
    }

    // 发送终止任务给所有工作线程
    for (unsigned i = 0; i < js->threadNum; ++i) {
        std::lock_guard<std::mutex> lock(queueMtx);
        [[maybe_unused]] bool result = jobQueue->enQueue(nullptr);
        condNonEmpty.notify_one();
    }

    // 广播确保每个工作线程都能收到终止信号
    {
        std::lock_guard<std::mutex> lock(queueMtx);
        condNonEmpty.notify_all();
    }

    // 等待所有工作线程结束
    for (auto& thread : js->threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }

    // 销毁任务队列
    delete jobQueue;
    jobQueue = nullptr;

    // 销毁分区互斥锁数组
    delete[] partitionMtxArray;
    partitionMtxArray = nullptr;

    // 重置线程同步变量
    barrierCount = 0;
    barrierTotal = 0;

    // 释放直方图数组
    for (unsigned i = 0; i < js->threadNum; ++i) {
        delete[] js->histArray[i];
    }
    js->histArray.clear();

    // 释放各种任务数组的参数内存
    // 注意：这里假设参数是用 new 分配的
    for (unsigned i = 0; i < js->threadNum; ++i) {
        delete static_cast<HistArg*>(js->histJobs[i].argument);
    }
    delete[] js->histJobs;

    for (unsigned i = 0; i < js->threadNum; ++i) {
        delete static_cast<PartitionArg*>(js->partitionJobs[i].argument);
    }
    delete[] js->partitionJobs;

    for (unsigned i = 0; i < HASH_RANGE_1; ++i) {
        delete static_cast<BuildArg*>(js->buildJobs[i].argument);
    }
    delete[] js->buildJobs;

    for (unsigned i = 0; i < HASH_RANGE_1; ++i) {
        delete static_cast<JoinArg*>(js->joinJobs[i].argument);
    }
    delete[] js->joinJobs;

    for (unsigned i = 0; i < HASH_RANGE_1; ++i) {
        delete static_cast<ColEqualityArg*>(js->colEqualityJobs[i].argument);
    }
    delete[] js->colEqualityJobs;

    for (unsigned i = 0; i < HASH_RANGE_1; ++i) {
        delete static_cast<FilterArg*>(js->filterJobs[i].argument);
    }
    delete[] js->filterJobs;

    for (unsigned i = 0; i < HASH_RANGE_1; ++i) {
        delete static_cast<CheckSumArg*>(js->checkSumJobs[i].argument);
    }
    delete[] js->checkSumJobs;

    // 销毁 JobScheduler 实例
    delete js;
    
    // 重置全局指针
    radix_join::js = nullptr;
}

/*==============================================================================
 * 任务提交和同步
 *============================================================================*/

void submitJob(Job* job) {
    std::lock_guard<std::mutex> lock(queueMtx);
    [[maybe_unused]] bool result = jobQueue->enQueue(static_cast<void*>(job));
    condNonEmpty.notify_one();
}

void waitUntilJobsFinished(unsigned expectedCount) {
    std::unique_lock<std::mutex> lock(jobsFinishedMtx);
    // 等待直到指定数量的任务完成
    condJobsFinished.wait(lock, [expectedCount]() {
        return jobsFinished >= expectedCount;
    });
    // 重置计数器以便下次使用
    jobsFinished = 0;
}

} // namespace radix_join
