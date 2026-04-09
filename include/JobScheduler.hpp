#pragma once

#include <cstdint>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <vector>
#include <memory>

namespace radix_join {

// 前向声明
class Queue;
class JobScheduler;

/*==============================================================================
 * 全局同步变量 - 命名空间级别
 * 这些变量被 Partition, Build, Probe, Operations, Vector 等模块的线程函数直接引用
 *============================================================================*/

// 队列互斥锁 - 保护工作队列的访问
extern std::mutex queueMtx;

// 任务完成计数互斥锁
extern std::mutex jobsFinishedMtx;

// 队列非空条件变量 - 通知工作线程有新任务
extern std::condition_variable condNonEmpty;

// 所有任务完成条件变量 - 通知主线程所有任务已完成
extern std::condition_variable condJobsFinished;

// 线程同步变量 - 用于 histogram/partition 阶段的线程同步
extern std::mutex barrierMtx;
extern std::condition_variable barrierCond;
extern unsigned barrierCount;
extern unsigned barrierTotal;
extern unsigned barrierGeneration;  // 代计数器，确保barrier可安全重用

// 任务队列指针 - 所有线程共享
extern Queue* jobQueue;

// JobScheduler 全局指针
extern JobScheduler* js;

// 分区互斥锁数组 - 用于保护分区操作
extern std::mutex* partitionMtxArray;

// 已完成任务计数
extern unsigned jobsFinished;

/*==============================================================================
 * 线程数量常量
 *============================================================================*/
inline constexpr unsigned THREAD_NUM = 4;

/*==============================================================================
 * Job 结构体 - 保持 C 风格函数指针形式以确保兼容性
 * 所有线程工作函数（histFunc, partitionFunc, buildFunc, joinFunc 等）
 * 都使用 void(*)(void*) 签名
 *============================================================================*/
struct Job {
    // 工作线程将要执行的函数
    void (*function)(void*);
    // 传递给函数的参数
    void* argument;

    // 构造函数
    Job() : function(nullptr), argument(nullptr) {}
    Job(void (*func)(void*), void* arg)
        : function(func), argument(arg) {}
};

/*==============================================================================
 * JobScheduler 类 - 任务调度器
 * 管理工作线程池和各种类型的任务数组
 *============================================================================*/
class JobScheduler {
public:
    // 工作线程数量
    unsigned threadNum;

    // 工作线程数组 - 使用 std::vector<std::thread> 替代 pthread_t*
    std::vector<std::thread> threads;

    // 直方图数组 - 每个线程一个直方图
    std::vector<unsigned*> histArray;

    // 校验和数组
    std::vector<uint64_t> checkSumArray;

    // 各种类型的任务数组
    Job* histJobs;           // 直方图计算任务
    Job* partitionJobs;      // 分区任务
    Job* buildJobs;          // 构建索引任务
    Job* joinJobs;           // 连接任务
    Job* colEqualityJobs;    // 列相等性检查任务
    Job* filterJobs;         // 过滤任务
    Job* checkSumJobs;       // 校验和计算任务

    // 构造函数
    JobScheduler() = default;

    // 析构函数
    ~JobScheduler() = default;

    // 禁止拷贝
    JobScheduler(const JobScheduler&) = delete;
    JobScheduler& operator=(const JobScheduler&) = delete;

    // 允许移动
    JobScheduler(JobScheduler&&) noexcept = default;
    JobScheduler& operator=(JobScheduler&&) noexcept = default;
};

/*==============================================================================
 * 函数声明
 *============================================================================*/

/**
 * @brief 创建 JobScheduler 实例
 * 初始化工作线程、任务队列、同步原语和各种任务数组
 * 
 * @param js 指向 JobScheduler 指针的引用，用于返回创建的实例
 */
void createJobScheduler(JobScheduler** js);

/**
 * @brief 创建各种类型的任务数组
 * 为 histogram、partition、build、join 等操作预创建任务对象
 * 
 * @param js JobScheduler 实例指针
 */
void createJobArrays(JobScheduler* js);

/**
 * @brief 工作线程函数
 * 从任务队列中获取任务并执行，直到收到 nullptr 任务时退出
 * 
 * @param arg 线程参数（未使用，保持兼容性）
 */
void threadFunc(void* arg);

/**
 * @brief 销毁 JobScheduler 实例
 * 发送终止信号给所有工作线程，等待线程结束，释放所有资源
 * 
 * @param js JobScheduler 实例指针
 */
void destroyJobScheduler(JobScheduler* js);

/**
 * @brief 提交任务到队列
 * 将任务添加到工作队列并通知工作线程
 * 
 * @param job 要提交的任务指针
 */
void submitJob(Job* job);

/**
 * @brief 等待指定数量的任务完成
 * 阻塞直到指定数量的任务被执行完毕
 * 
 * @param expectedCount 期望完成的任务数量
 */
void waitUntilJobsFinished(unsigned expectedCount);

} // namespace radix_join
