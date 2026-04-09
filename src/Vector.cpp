#include "Vector.hpp"
#include "Utils.hpp"
#include "Intermediate.hpp"
#include "JobScheduler.hpp"
#include "Joiner.hpp"
#include <cstring>
#include <stdexcept>
#include <cstdlib>  // for aligned_alloc

namespace radix_join {

//=============================================================================
// 内存对齐辅助函数
//=============================================================================

/**
 * @brief 分配128字节对齐的内存
 * 
 * 使用 aligned_alloc 分配指定大小的内存，对齐到128字节边界。
 * 这对于缓存敏感的 hash join 操作有性能优势。
 * 
 * @param size 需要分配的字节数
 * @return 对齐的内存指针
 * @throws std::runtime_error 如果分配失败
 */
static unsigned* alignedAlloc128(size_t size) {
    // aligned_alloc 要求大小是alignment的倍数
    size_t alignedSize = (size + 127) & ~127;  // 向上取整到128的倍数
    void* ptr = std::aligned_alloc(128, alignedSize);
    if (!ptr) {
        throw std::runtime_error("[ERROR] aligned_alloc failed for 128-byte aligned memory");
    }
    return static_cast<unsigned*>(ptr);
}

/**
 * @brief 释放128字节对齐的内存
 * 
 * @param ptr 要释放的内存指针
 */
static void alignedFree(void* ptr) {
    std::free(ptr);  // C11 aligned_alloc 分配的内存可以用 free 释放
}

//=============================================================================
// Vector 类构造函数和析构函数
//=============================================================================

Vector::Vector(unsigned tupleSize) 
    : table(nullptr)
    , tupleSize(tupleSize)
    , nextPos(0)
    , capacity(0) 
{
}

Vector::Vector(unsigned tupleSize, unsigned fixedSize)
    : tupleSize(tupleSize)
    , nextPos(0)
{
    // 固定大小 Vector：预先分配所需的所有空间
    capacity = tupleSize * fixedSize;
    // 使用128字节对齐的内存分配
    table = alignedAlloc128(capacity * sizeof(unsigned));
}

Vector::~Vector() {
    clear();
}

Vector::Vector(Vector&& other) noexcept
    : table(other.table)
    , tupleSize(other.tupleSize)
    , nextPos(other.nextPos)
    , capacity(other.capacity)
{
    // 将源对象置空，避免双重释放
    other.table = nullptr;
    other.tupleSize = 0;
    other.nextPos = 0;
    other.capacity = 0;
}

Vector& Vector::operator=(Vector&& other) noexcept {
    if (this != &other) {
        // 释放当前资源
        clear();
        
        // 移动资源
        table = other.table;
        tupleSize = other.tupleSize;
        nextPos = other.nextPos;
        capacity = other.capacity;
        
        // 将源对象置空
        other.table = nullptr;
        other.tupleSize = 0;
        other.nextPos = 0;
        other.capacity = 0;
    }
    return *this;
}

//=============================================================================
// Vector 类成员函数
//=============================================================================

void Vector::insert(const unsigned* tuple) {
    // 如果 Vector 为空（未分配 table），则创建 table
    if (isEmpty()) {
        // initSize 由 Joiner 模块根据平均元组数初始化
        capacity = initSize * tupleSize;
        table = alignedAlloc128(capacity * sizeof(unsigned));
    }
    // 如果 Vector 已满，则扩容（容量翻倍）
    else if (isFull()) {
        capacity *= 2;
        unsigned* newTable = alignedAlloc128(capacity * sizeof(unsigned));
        // 复制旧数据到新数组
        std::memcpy(newTable, table, nextPos * sizeof(unsigned));
        // 释放旧数组
        alignedFree(table);
        table = newTable;
    }

    // 插入元组数据
    unsigned pos = nextPos;
    for (unsigned i = 0; i < tupleSize; ++i) {
        table[pos + i] = tuple[i];
    }
    nextPos += tupleSize;
}

void Vector::insertAt(const unsigned* tuple, unsigned offset) {
    // 计算在 table 中的实际位置
    unsigned pos = offset * tupleSize;
    for (unsigned i = 0; i < tupleSize; ++i) {
        table[pos + i] = tuple[i];
    }
    nextPos += tupleSize;
}

bool Vector::isFull() const noexcept {
    return nextPos == capacity;
}

bool Vector::isEmpty() const noexcept {
    return table == nullptr;
}

unsigned Vector::getTupleCount() const noexcept {
    return nextPos / tupleSize;
}

unsigned Vector::getTupleSize() const noexcept {
    return tupleSize;
}

unsigned* Vector::getTuple(unsigned i) {
    if (i > getTupleCount()) {
        throw std::runtime_error("Trying to access a tuple that does not exist");
    }
    return &table[i * tupleSize];
}

const unsigned* Vector::getTuple(unsigned i) const {
    if (i > getTupleCount()) {
        throw std::runtime_error("Trying to access a tuple that does not exist");
    }
    return &table[i * tupleSize];
}

void Vector::print() const {
    if (nextPos == 0) {
        return;
    }
    
    unsigned k = 0;
    for (unsigned i = 0; i < nextPos; ++i) {
        if (i % tupleSize == 0) {
            if (k++ == 10) {  // 最多打印前10个元组
                break;
            }
            printTuple(i);
        }
    }
}

void Vector::printTuple(unsigned pos) const {
    std::cerr << "(";
    for (unsigned i = 0; i < tupleSize; ++i) {
        if (i == tupleSize - 1) {
            std::cerr << (table[pos + i] + 1);  // +1 是为了显示时从1开始计数
        } else {
            std::cerr << (table[pos + i] + 1) << ",";
        }
    }
    std::cerr << ")" << std::endl;
}

uint64_t Vector::checkSum(const uint64_t* col, unsigned rowIdPos) const {
    uint64_t sum = 0;
    for (unsigned i = 0; i < nextPos; i += tupleSize) {
        sum += col[table[i + rowIdPos]];
    }
    return sum;
}

void Vector::clear() {
    if (table) {
        alignedFree(table);
    }
    table = nullptr;
    tupleSize = 0;
    nextPos = 0;
    capacity = 0;
}

//=============================================================================
// 线程工作函数
//=============================================================================

void checkSumFunc(void* arg) {
    auto* myarg = static_cast<CheckSumArg*>(arg);
    
    *myarg->sum = 0;
    for (unsigned i = 0; i < myarg->vector->nextPos; i += myarg->vector->tupleSize) {
        *myarg->sum += myarg->col[myarg->vector->table[i + myarg->rowIdPos]];
    }
    
    // 通知 JobScheduler 任务完成
    std::lock_guard<std::mutex> lock(jobsFinishedMtx);
    ++jobsFinished;
    condJobsFinished.notify_one();
}

void colEqualityFunc(void* arg) {
    auto* myarg = static_cast<ColEqualityArg*>(arg);
    
    for (unsigned i = 0; i < myarg->oldVector->nextPos; i += myarg->oldVector->tupleSize) {
        unsigned leftRowId = myarg->oldVector->table[i + myarg->posLeft];
        unsigned rightRowId = myarg->oldVector->table[i + myarg->posRight];
        if (myarg->leftCol[leftRowId] == myarg->rightCol[rightRowId]) {
            myarg->newVector->insert(&myarg->oldVector->table[i]);
        }
    }
    
    // 通知 JobScheduler 任务完成
    std::lock_guard<std::mutex> lock(jobsFinishedMtx);
    ++jobsFinished;
    condJobsFinished.notify_one();
}

//=============================================================================
// 扫描过滤函数
//=============================================================================

void scanColEquality(Vector* newVec, Vector* oldVec,
                     const uint64_t* leftCol, const uint64_t* rightCol,
                     unsigned posLeft, unsigned posRight) {
    // 注意：oldVec 中的每个元组包含一个或多个 rowId
    // 对于来自同一中间实体的关系的 join 场景，元组可能包含多个 rowId
    for (unsigned i = 0; i < oldVec->nextPos; i += oldVec->tupleSize) {
        unsigned leftRowId = oldVec->table[i + posLeft];
        unsigned rightRowId = oldVec->table[i + posRight];
        if (leftCol[leftRowId] == rightCol[rightRowId]) {
            newVec->insert(&oldVec->table[i]);
        }
    }
}

void scanFilter(Vector* newVec, Vector* oldVec,
                const uint64_t* col, Comparison cmp, uint64_t constant) {
    // 扫描 oldVec，将满足过滤条件的元组插入 newVec
    for (unsigned i = 0; i < oldVec->nextPos; i += oldVec->tupleSize) {
        unsigned rowId = oldVec->table[i];
        if (compare(col[rowId], cmp, constant)) {
            newVec->insert(&oldVec->table[i]);
        }
    }
}

//=============================================================================
// C 风格兼容函数实现
//=============================================================================

void createVector(Vector** vector, unsigned tupleSize) {
    *vector = new Vector(tupleSize);
}

void createVectorFixedSize(Vector** vector, unsigned tupleSize, unsigned fixedSize) {
    *vector = new Vector(tupleSize, fixedSize);
}

void insertAtVector(Vector* vector, unsigned* tuple) {
    vector->insert(tuple);
}

void insertAtPos(Vector* vector, unsigned* tuple, unsigned offset) {
    vector->insertAt(tuple, offset);
}

void scanJoin(RadixHashJoinInfo* joinRel) {
    // 检查必要的指针是否为nullptr
    if (!joinRel || !joinRel->unsorted || !joinRel->unsorted->tuples || !joinRel->map) {
        throw std::runtime_error("[ERROR] Null pointer in scanJoin");
    }
    
    // 获取 unsorted 的 tuples Vector
    Vector* newVec = joinRel->unsorted->tuples;
    
    // 在 tuple 中的位置偏移
    unsigned tupleOffset = joinRel->map[joinRel->relId];
    
    // 检查 col、values 和 rowIds 是否为nullptr
    if (!joinRel->col || !joinRel->unsorted->values || !joinRel->unsorted->rowIds) {
        throw std::runtime_error("[ERROR] Null pointer in scanJoin: missing data arrays");
    }
    
    uint64_t* origValues = joinRel->col;
    uint64_t* colValues = joinRel->unsorted->values;
    unsigned* rowIds = joinRel->unsorted->rowIds;
    
    unsigned k = 0;
    // 扫描旧的 vectors（中间结果）
    for (unsigned v = 0; v < HASH_RANGE_1; ++v) {
        if (joinRel->vector && joinRel->vector[v] != nullptr) {
            Vector* oldVec = joinRel->vector[v];
            if (oldVec->table) {
                for (unsigned i = 0; i < oldVec->nextPos; i += oldVec->tupleSize) {
                    unsigned origRowId = oldVec->table[i + tupleOffset];
                    // 添加值
                    colValues[k] = origValues[origRowId];
                    // 添加元组
                    newVec->insert(&oldVec->table[i]);
                    // 添加行ID（先赋值再递增）
                    rowIds[k] = k + 1;
                    ++k;
                }
            }
        }
    }
}

void destroyVector(Vector** vector) {
    if (vector && *vector) {
        delete *vector;
        *vector = nullptr;
    }
}

unsigned getTupleSize(Vector* vector) {
    return vector ? vector->getTupleSize() : 0;
}

} // namespace radix_join
