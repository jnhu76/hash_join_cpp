#include "Queue.hpp"

#include <iostream>

namespace radix_join {

Queue::Queue(int size)
    : array_(static_cast<std::size_t>(size), nullptr)
    , front_(-1)
    , rear_(-1)
    , capacity_(size) {
    if (size <= 0) {
        throw std::invalid_argument("Queue size must be positive");
    }
}

bool Queue::enQueue(void* item) {
    // 检查队列是否已满
    // 条件1: rear在末尾且front在开头（完全填满）
    // 条件2: rear在front前面一个位置（循环填满）
    if (((rear_ == capacity_ - 1) && (front_ == 0)) || 
        (rear_ == (front_ - 1) % (capacity_ - 1))) {
        std::cerr << "Circular Queue is full\n";
        return false;
    }

    // 第一个元素入队
    if (front_ == -1) {
        rear_ = 0;
        front_ = 0;
        array_[0] = item;
    }
    // rear到达末尾但前面还有空间，循环到开头
    else if ((rear_ == capacity_ - 1) && (front_ != 0)) {
        rear_ = 0;
        array_[rear_] = item;
    }
    // 正常情况：直接插入到下一个位置
    else {
        array_[++rear_] = item;
    }

    return true;
}

void* Queue::deQueue() {
    // 队列为空
    if (front_ == -1) {
        std::cerr << "Circular Queue is empty\n";
        return nullptr;
    }

    void* value = array_[front_];

    // 队列只有一个元素
    if (front_ == rear_) {
        front_ = -1;
        rear_ = -1;
    }
    // front到达末尾，循环到开头
    else if (front_ == capacity_ - 1) {
        front_ = 0;
    }
    // 正常情况：移动到下一个元素
    else {
        ++front_;
    }

    return value;
}

bool Queue::isEmpty() const noexcept {
    return front_ == -1;
}

bool Queue::isFull() const noexcept {
    return ((rear_ == capacity_ - 1) && (front_ == 0)) || 
           (rear_ == (front_ - 1) % (capacity_ - 1));
}

int Queue::getSize() const noexcept {
    if (front_ == -1) {
        return 0;
    }
    if (rear_ >= front_) {
        return rear_ - front_ + 1;
    }
    return capacity_ - front_ + rear_ + 1;
}

int Queue::getCapacity() const noexcept {
    return capacity_;
}

void Queue::display() const {
    std::cout << "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n";
    std::cerr << "front is: " << front_ << "\n";
    std::cerr << "rear is: " << rear_ << "\n";
    for (int i = 0; i < capacity_; ++i) {
        std::cerr << array_[i] << "| ";
    }
    std::cerr << "\n";
    std::cerr << "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n";
}

} // namespace radix_join
