#include <iostream>
#include <string>
#include <sstream>

#include "Joiner.hpp"
#include "Parser.hpp"
#include "Intermediate.hpp"

namespace radix_join {

// 全局变量定义 - 这些被Partition、Build、Probe、Vector等模块引用
unsigned RADIX_BITS = 4;      // 默认值，会被setRadixBits覆盖
unsigned HASH_RANGE_1 = 16;   // 默认值，会被setRadixBits覆盖
unsigned initSize = 1000;     // 默认值，会被setVectorInitSize覆盖

void Joiner::setup() {
    // 存储所有文件名的缓冲区
    std::string buffer;
    buffer.reserve(BUFFERSIZE);
    
    std::string fileName;
    std::vector<std::string> fileNames;
    
    std::cerr << "[DEBUG] Reading file names from stdin..." << std::endl;
    
    // 从标准输入读取文件名，直到遇到"Done"
    while (std::getline(std::cin, fileName)) {
        std::cerr << "[DEBUG] Read: '" << fileName << "'" << std::endl;
        if (fileName == "Done") {
            break;
        }
        // 移除可能的换行符
        if (!fileName.empty() && fileName.back() == '\n') {
            fileName.pop_back();
        }
        if (!fileName.empty() && fileName.back() == '\r') {
            fileName.pop_back();
        }
        if (!fileName.empty()) {
            fileNames.push_back(fileName);
        }
    }
    
    std::cerr << "[DEBUG] Read " << fileNames.size() << " file names." << std::endl;
    
    // 预分配关系数组空间
    relations_.reserve(fileNames.size());
    
    // 添加每个关系
    for (const auto& name : fileNames) {
        std::cerr << "[DEBUG] Loading relation: " << name << std::endl;
        addRelation(name);
    }
    
    std::cerr << "[DEBUG] All relations loaded." << std::endl;
    
    // 根据数据大小设置参数
    setRadixBits();
    setVectorInitSize();
}

void Joiner::addRelation(const std::string& fileName) {
    // 创建新关系并加载文件
    auto rel = std::make_unique<Relation>(fileName);
    relations_.push_back(std::move(rel));
}

void Joiner::join(const QueryInfo& q) {
    InterMetaData* inter;
    createInterMetaData(&inter, const_cast<QueryInfo*>(&q));

    // 应用列等值谓词
    applyColumnEqualities(inter, this, const_cast<QueryInfo*>(&q));

    // 应用过滤器
    applyFilters(inter, this, const_cast<QueryInfo*>(&q));

    // 应用连接
    applyJoins(inter, this, const_cast<QueryInfo*>(&q));

    // 计算校验和
    applyCheckSums(inter, this, const_cast<QueryInfo*>(&q));

    // 销毁中间结果
    destroyInterMetaData(inter);
}

void Joiner::setVectorInitSize() {
    /**
     * 根据平均元组数设置Vector初始大小：
     * - small (< 500K):    initSize = 1000
     * - medium (< 1.2M):   initSize = 1000
     * - large (< 2M):      initSize = 5000
     * - public (>= 2M):    initSize = 500000
     */
    
    if (relations_.empty()) {
        initSize = 1000;
        return;
    }
    
    unsigned sum = 0;
    for (const auto& rel : relations_) {
        sum += rel->numOfTuples;
    }
    unsigned avgNumOfTuples = sum / static_cast<unsigned>(relations_.size());
    
    if (avgNumOfTuples < 500000) {
        initSize = 1000;
    } else if (avgNumOfTuples < 1200000) {
        initSize = 1000;
    } else if (avgNumOfTuples < 2000000) {
        initSize = 5000;
    } else {
        initSize = 500000;
    }
}

void Joiner::setRadixBits() {
    /**
     * 根据平均元组数设置基数哈希参数：
     * - small (< 500K):    RADIX_BITS = 4, HASH_RANGE_1 = 16
     * - medium (< 2M):     RADIX_BITS = 5, HASH_RANGE_1 = 32
     * - large (>= 2M):     RADIX_BITS = 8, HASH_RANGE_1 = 256
     */
    
    if (relations_.empty()) {
        RADIX_BITS = 4;
        HASH_RANGE_1 = 16;
        return;
    }
    
    unsigned sum = 0;
    for (const auto& rel : relations_) {
        sum += rel->numOfTuples;
    }
    unsigned avgNumOfTuples = sum / static_cast<unsigned>(relations_.size());
    
    if (avgNumOfTuples < 500000) {
        RADIX_BITS   = 4;
        HASH_RANGE_1 = 16;
    } else if (avgNumOfTuples < 2000000) {
        RADIX_BITS   = 5;
        HASH_RANGE_1 = 32;
    } else {
        RADIX_BITS   = 8;
        HASH_RANGE_1 = 256;
    }
}

} // namespace radix_join
