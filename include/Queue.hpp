#pragma once

#include <vector>
#include <cstddef>
#include <stdexcept>
#include <iostream>

namespace radix_join {

/**
 * @brief 循环队列类
 * 使用 std::vector<void*> 作为底层存储的循环队列实现
 * 保持与原始C代码相同的语义，但使用C++现代特性
 * 
 * 注意：队列存储 void* 指针，由调用者管理指针生命周期
 */
class Queue {
public:
    /**
     * @brief 构造函数
     * @param size 队列容量
     */
    explicit Queue(int size);

    /**
     * @brief 析构函数
     * 自动释放内部存储，不释放存储的指针
     */
    ~Queue() = default;

    // 禁止拷贝和赋值（原始C代码语义）
    Queue(const Queue&) = delete;
    Queue& operator=(const Queue&) = delete;

    // 允许移动语义
    Queue(Queue&&) noexcept = default;
    Queue& operator=(Queue&&) noexcept = default;

    /**
     * @brief 入队操作
     * 将元素添加到队列尾部
     * 
     * @param item 待添加的元素指针
     * @return true 入队成功
     * @return false 队列已满
     */
    [[nodiscard]] bool enQueue(void* item);

    /**
     * @brief 出队操作
     * 从队列头部移除并返回元素
     * 
     * @return void* 队列头部元素，队列为空时返回 nullptr
     */
    [[nodiscard]] void* deQueue();

    /**
     * @brief 检查队列是否为空
     * @return true 队列为空
     * @return false 队列非空
     */
    [[nodiscard]] bool isEmpty() const noexcept;

    /**
     * @brief 检查队列是否已满
     * @return true 队列已满
     * @return false 队列未满
     */
    [[nodiscard]] bool isFull() const noexcept;

    /**
     * @brief 获取队列当前元素数量
     * @return int 当前元素数量
     */
    [[nodiscard]] int getSize() const noexcept;

    /**
     * @brief 获取队列容量
     * @return int 队列容量
     */
    [[nodiscard]] int getCapacity() const noexcept;

    /**
     * @brief 显示队列状态（调试用）
     * 输出队列的前端、后端位置和元素
     */
    void display() const;

private:
    std::vector<void*> array_;  // 存储元素的数组
    int front_;                 // 队列前端索引，-1表示空
    int rear_;                  // 队列后端索引，-1表示空
    int capacity_;              // 队列容量
};

/*==============================================================================
 * C 风格兼容函数 - 用于与 Partition/Build/Probe 模块兼容
 *============================================================================*/

/**
 * @brief 入队操作（C 风格兼容函数）
 * @param q 队列指针
 * @param item 要入队的元素
 * @return 1 成功，0 失败
 */
inline int enQueue(Queue* q, void* item) {
    return q->enQueue(item) ? 1 : 0;
}

/**
 * @brief 出队操作（C 风格兼容函数）
 * @param q 队列指针
 * @return 出队的元素，失败返回 nullptr
 */
inline void* deQueue(Queue* q) {
    return q->deQueue();
}

/**
 * @brief 检查队列是否为空（C 风格兼容函数）
 * @param q 队列指针
 * @return 1 为空，0 非空
 */
inline int isEmpty(Queue* q) {
    return q->isEmpty() ? 1 : 0;
}

} // namespace radix_join
