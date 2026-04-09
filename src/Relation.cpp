#include "Relation.hpp"
#include "Optimizer.hpp"
#include "Utils.hpp"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <fstream>
#include <stdexcept>
#include <cstring>
#include <limits>

namespace radix_join {

//=============================================================================
// Relation 类构造函数和析构函数
//=============================================================================

Relation::Relation(const std::string& fileName) {
    DEBUG_LOG("[DEBUG] Relation::Relation(" << fileName << ")");
    loadFromFile(fileName);
    
    DEBUG_LOG("[DEBUG] loadFromFile done");
    
    // 分配统计信息数组空间
    stats = new ColumnStats[numOfCols]();
    if (!stats) {
        throw std::runtime_error("[ERROR] Memory allocation failed for stats");
    }
    
    DEBUG_LOG("[DEBUG] Calculating stats...");
    // 计算每列的统计信息
    calculateStats();
    DEBUG_LOG("[DEBUG] Stats calculated.");
}

Relation::~Relation() {
    // 释放每列的位图（如果已分配）
    if (stats) {
        for (unsigned i = 0; i < numOfCols; ++i) {
            if (stats[i].bitVector) {
                delete[] stats[i].bitVector;
                stats[i].bitVector = nullptr;
            }
        }
        delete[] stats;
    }
    
    // 释放列指针数组
    delete[] columns;
    
    // 显式释放mmap的内存
    if (mmapAddr != nullptr) {
        munmap(mmapAddr, mmapSize);
        mmapAddr = nullptr;
        mmapSize = 0;
    }
}

Relation::Relation(Relation&& other) noexcept
    : numOfTuples(other.numOfTuples)
    , numOfCols(other.numOfCols)
    , columns(other.columns)
    , stats(other.stats)
    , mmapAddr(other.mmapAddr)
    , mmapSize(other.mmapSize)
{
    // 将源对象置空
    other.numOfTuples = 0;
    other.numOfCols = 0;
    other.columns = nullptr;
    other.stats = nullptr;
    other.mmapAddr = nullptr;
    other.mmapSize = 0;
}

Relation& Relation::operator=(Relation&& other) noexcept {
    if (this != &other) {
        // 释放当前资源
        if (stats) {
            for (unsigned i = 0; i < numOfCols; ++i) {
                if (stats[i].bitVector) {
                    delete[] stats[i].bitVector;
                    stats[i].bitVector = nullptr;
                }
            }
            delete[] stats;
        }
        delete[] columns;
        if (mmapAddr != nullptr) {
            munmap(mmapAddr, mmapSize);
            mmapAddr = nullptr;
            mmapSize = 0;
        }
        
        // 移动资源
        numOfTuples = other.numOfTuples;
        numOfCols = other.numOfCols;
        columns = other.columns;
        stats = other.stats;
        mmapAddr = other.mmapAddr;
        mmapSize = other.mmapSize;
        
        // 将源对象置空
        other.numOfTuples = 0;
        other.numOfCols = 0;
        other.columns = nullptr;
        other.stats = nullptr;
        other.mmapAddr = nullptr;
        other.mmapSize = 0;
    }
    return *this;
}

//=============================================================================
// 文件加载和导出
//=============================================================================

void Relation::loadFromFile(const std::string& fileName) {
    // 打开关系文件
    int fd = open(fileName.c_str(), O_RDONLY);
    if (fd == -1) {
        throw std::runtime_error("[ERROR] open failed in loadFromFile: " + fileName);
    }
    
    // 获取文件大小
    struct stat sb;
    if (fstat(fd, &sb) == -1) {
        close(fd);
        throw std::runtime_error("[ERROR] fstat failed in loadFromFile: " + fileName);
    }
    
    mmapSize = sb.st_size;
    if (mmapSize < 16) {
        close(fd);
        throw std::runtime_error("[ERROR] Relation file does not contain a valid header: " + fileName);
    }
    
    // 将文件映射到内存
    mmapAddr = static_cast<char*>(mmap(nullptr, mmapSize, PROT_READ, MAP_PRIVATE, fd, 0));
    if (mmapAddr == MAP_FAILED) {
        mmapAddr = nullptr;
        mmapSize = 0;
        close(fd);
        throw std::runtime_error("[ERROR] mmap failed in loadFromFile: " + fileName);
    }
    
    // 读取元组数量和列数量（文件头）
    numOfTuples = *reinterpret_cast<uint64_t*>(mmapAddr);
    char* addr = mmapAddr + sizeof(uint64_t);
    numOfCols = *reinterpret_cast<uint64_t*>(addr);
    addr += sizeof(uint64_t);
    
    // 分配列指针数组
    columns = new uint64_t*[numOfCols];
    if (!columns) {
        if (mmapAddr != nullptr) {
            munmap(mmapAddr, mmapSize);
            mmapAddr = nullptr;
            mmapSize = 0;
        }
        close(fd);
        throw std::runtime_error("[ERROR] Memory allocation failed for columns");
    }
    
    // 将每列映射到 columns 数组
    for (unsigned i = 0; i < numOfCols; ++i) {
        columns[i] = reinterpret_cast<uint64_t*>(addr);
        addr += numOfTuples * sizeof(uint64_t);
    }
    
    // 关闭文件（不影响 mmap，根据 man 页面说明）
    close(fd);
}

void Relation::dumpToFile(const std::string& fileName) const {
    // 构建文件路径
    std::string path = "../../dumpFiles/" + fileName + ".dump";
    
    // 使用 C++ 文件流创建文件
    std::ofstream ofs(path);
    if (!ofs) {
        throw std::runtime_error("[ERROR] Cannot create file in dumpToFile: " + path);
    }
    
    // 写入数据
    for (unsigned i = 0; i < numOfTuples; ++i) {
        for (unsigned j = 0; j < numOfCols; ++j) {
            ofs << columns[j][i];
            if (j < numOfCols - 1) {
                ofs << "|";
            }
        }
        ofs << "\n";
    }
}

void Relation::print() const {
    for (unsigned i = 0; i < numOfTuples; ++i) {
        for (unsigned j = 0; j < numOfCols; ++j) {
            std::cout << columns[j][i];
            if (j < numOfCols - 1) {
                std::cout << "|";
            }
        }
        std::cout << "\n";
    }
}

//=============================================================================
// 统计信息计算
//=============================================================================

void Relation::findStats(const uint64_t* column, ColumnStats* stat, unsigned columnSize) {
    // 调用 Optimizer 模块中正确的 findStats 实现
    // 避免重复实现导致的不一致性
    radix_join::findStats(column, stat, columnSize);
}

void Relation::calculateStats() {
    DEBUG_LOG("[DEBUG] calculateStats: numOfCols=" << numOfCols);
    if (!stats || !columns) {
        return;
    }
    
    for (unsigned i = 0; i < numOfCols; ++i) {
        DEBUG_LOG("[DEBUG] Processing column " << i);
        findStats(columns[i], &stats[i], numOfTuples);
        DEBUG_LOG("[DEBUG] Column " << i << " done");
    }
    DEBUG_LOG("[DEBUG] calculateStats done");
}

//=============================================================================
// 独立函数接口（兼容原始C代码风格）
//=============================================================================

Relation loadRelation(const std::string& fileName) {
    return Relation(fileName);
}

void dumpRelation(const Relation& rel, const std::string& fileName) {
    rel.dumpToFile(fileName);
}

void printRelation(const Relation& rel) {
    rel.print();
}

} // namespace radix_join
