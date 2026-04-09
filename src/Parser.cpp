#include <cstdlib>
#include <cstring>  // for memcpy
#include <iostream>  // for std::cerr
#include <sstream>  // for std::istringstream

#include "Parser.hpp"
#include "Joiner.hpp"
#include "Optimizer.hpp"  // for BITNSLOTS
#include "Utils.hpp"  // for Comparison

namespace radix_join {

// 辅助函数：将字符转换为比较枚举
static Comparison charToComparison(char c) {
    switch (c) {
        case '<': return Comparison::Less;
        case '>': return Comparison::Greater;
        case '=': return Comparison::Equal;
        default:
            throw std::runtime_error(std::string("Invalid comparison operator: ") + c);
    }
}

QueryInfo::QueryInfo(const std::string& rawQuery) {
    parseQuery(rawQuery);
}

void QueryInfo::parseQuery(const std::string& rawQuery) {
    // 查找两个分隔符 '|' 的位置
    size_t firstPipe = rawQuery.find('|');
    size_t secondPipe = rawQuery.find('|', firstPipe + 1);
    
    if (firstPipe == std::string::npos || secondPipe == std::string::npos) {
        throw std::runtime_error("Query \"" + rawQuery + "\" does not consist of three parts");
    }
    
    // 分割三个部分
    std::string rawRelations = rawQuery.substr(0, firstPipe);
    std::string rawPredicates = rawQuery.substr(firstPipe + 1, secondPipe - firstPipe - 1);
    std::string rawSelections = rawQuery.substr(secondPipe + 1);
    
    // 解析每个部分
    parseRelationIds(rawRelations);
    parsePredicates(rawPredicates);
    parseSelections(rawSelections);
}

void QueryInfo::parseRelationIds(const std::string& rawRelations) {
    relationIds.clear();
    
    std::istringstream iss(rawRelations);
    unsigned relId;
    
    // 读取所有关系ID
    while (iss >> relId) {
        relationIds.push_back(relId);
    }
    
    if (relationIds.empty()) {
        throw std::runtime_error("Zero join relations were found in the query");
    }
}

void QueryInfo::parseSelections(const std::string& rawSelections) {
    selections.clear();
    
    std::istringstream iss(rawSelections);
    std::string token;
    
    // 读取所有选择项，格式为 "relId.colId"
    while (iss >> token) {
        size_t dotPos = token.find('.');
        if (dotPos == std::string::npos) {
            throw std::runtime_error("Invalid selection format: " + token);
        }
        
        unsigned relId = static_cast<unsigned>(std::stoul(token.substr(0, dotPos)));
        unsigned colId = static_cast<unsigned>(std::stoul(token.substr(dotPos + 1)));
        
        selections.push_back(SelectInfo{relId, colId});
    }
    
    if (selections.empty()) {
        throw std::runtime_error("Zero selections were found in the query");
    }
}

void QueryInfo::parsePredicates(const std::string& rawPredicates) {
    predicates.clear();
    filters.clear();
    
    // 先统计过滤器和谓词数量
    std::vector<std::string> tokens;
    std::string temp = rawPredicates;
    size_t pos = 0;
    
    // 按 '&' 分割
    while ((pos = temp.find('&')) != std::string::npos) {
        std::string token = temp.substr(0, pos);
        if (!token.empty()) {
            tokens.push_back(token);
        }
        temp.erase(0, pos + 1);
    }
    // 添加最后一个标记
    if (!temp.empty()) {
        tokens.push_back(temp);
    }
    
    if (tokens.empty()) {
        throw std::runtime_error("Zero predicates were found in the query");
    }
    
    // 分类并解析每个标记
    for (const auto& token : tokens) {
        if (isFilter(token)) {
            FilterInfo fInfo;
            addFilter(fInfo, token);
            filters.push_back(fInfo);
        } else {
            PredicateInfo pInfo;
            addPredicate(pInfo, token);
            predicates.push_back(pInfo);
        }
    }
}

void addFilter(FilterInfo& fInfo, const std::string& token) {
    // 格式: "relId.colIdOpconstant"，如 "0.1>100"
    size_t dotPos = token.find('.');
    if (dotPos == std::string::npos) {
        throw std::runtime_error("Invalid filter format: " + token);
    }
    
    unsigned relId = static_cast<unsigned>(std::stoul(token.substr(0, dotPos)));
    
    // 查找比较操作符位置（<, >, =）
    size_t opPos = token.find_first_of("<>=", dotPos + 1);
    if (opPos == std::string::npos) {
        throw std::runtime_error("Invalid filter format (no operator): " + token);
    }
    
    unsigned colId = static_cast<unsigned>(std::stoul(token.substr(dotPos + 1, opPos - dotPos - 1)));
    char cmp = token[opPos];
    uint64_t constant = std::stoull(token.substr(opPos + 1));
    
    fInfo.filterLhs = SelectInfo{relId, colId};
    fInfo.comparison = charToComparison(cmp);
    fInfo.constant = constant;
}

void addPredicate(PredicateInfo& pInfo, const std::string& token) {
    // 格式: "relId1.colId1=relId2.colId2"，如 "0.1=1.2"
    size_t eqPos = token.find('=');
    if (eqPos == std::string::npos) {
        throw std::runtime_error("Invalid predicate format: " + token);
    }
    
    std::string left = token.substr(0, eqPos);
    std::string right = token.substr(eqPos + 1);
    
    // 解析左侧
    size_t leftDot = left.find('.');
    unsigned leftRelId = static_cast<unsigned>(std::stoul(left.substr(0, leftDot)));
    unsigned leftColId = static_cast<unsigned>(std::stoul(left.substr(leftDot + 1)));
    
    // 解析右侧
    size_t rightDot = right.find('.');
    unsigned rightRelId = static_cast<unsigned>(std::stoul(right.substr(0, rightDot)));
    unsigned rightColId = static_cast<unsigned>(std::stoul(right.substr(rightDot + 1)));
    
    pInfo.left = SelectInfo{leftRelId, leftColId};
    pInfo.right = SelectInfo{rightRelId, rightColId};
}

bool isFilter(const std::string& predicate) {
    // 查找比较操作符
    size_t opPos = predicate.find_first_of("<>=");
    if (opPos == std::string::npos) {
        return false;
    }
    
    // 获取操作符后的部分
    std::string afterOp = predicate.substr(opPos + 1);
    
    // 如果包含 '.'，则是连接谓词（如 "0.1=1.2"）
    // 如果不包含 '.'，则是过滤器（如 "0.1>100"）
    return afterOp.find('.') == std::string::npos;
}

void QueryInfo::createQueryEstimations(const Joiner& joiner) {
    // 为每个关系分配统计信息数组
    estimations.resize(relationIds.size());
    
    for (size_t i = 0; i < relationIds.size(); ++i) {
        unsigned relId = relationIds[i];
        const Relation* rel = joiner.getRelation(relId);
        
        // 分配空间存储估计统计信息
        estimations[i].resize(rel->numOfCols);
        
        // 复制加载关系时计算的统计信息
        for (unsigned c = 0; c < rel->numOfCols; ++c) {
            estimations[i][c] = rel->stats[c];
            // 需要深拷贝位向量，因为ColumnStats默认拷贝会共享指针
            if (rel->stats[c].bitVector != nullptr) {
                size_t bvSize = BITNSLOTS(rel->stats[c].bitVectorSize);
                estimations[i][c].bitVector = static_cast<char*>(malloc(bvSize));
                if (estimations[i][c].bitVector) {
                    memcpy(estimations[i][c].bitVector, rel->stats[c].bitVector, bvSize);
                }
            }
        }
    }
}

unsigned QueryInfo::getNumOfColEqualities() const {
    unsigned sum = 0;
    for (const auto& pred : predicates) {
        if (isColEquality(pred)) {
            ++sum;
        }
    }
    return sum;
}

unsigned QueryInfo::getNumOfJoins() const {
    unsigned sum = 0;
    for (const auto& pred : predicates) {
        if (!isColEquality(pred)) {
            ++sum;
        }
    }
    return sum;
}

void QueryInfo::print() const {
    // 打印关系ID
    for (unsigned relId : relationIds) {
        std::cerr << relId << " ";
    }
    std::cerr << "|";
    
    // 打印谓词
    for (const auto& pred : predicates) {
        unsigned leftRelId = getRelId(pred.left);
        unsigned rightRelId = getRelId(pred.right);
        unsigned leftColId = getColId(pred.left);
        unsigned rightColId = getColId(pred.right);
        
        if (isColEquality(pred)) {
            std::cerr << "[" << leftRelId << "." << leftColId << "=" 
                      << rightRelId << "." << rightColId << "] & ";
        } else {
            std::cerr << leftRelId << "." << leftColId << "=" 
                      << rightRelId << "." << rightColId << " & ";
        }
    }
    
    // 打印过滤器
    for (const auto& filter : filters) {
        unsigned relId = getRelId(filter.filterLhs);
        unsigned colId = getColId(filter.filterLhs);
        char cmp = static_cast<char>(filter.comparison);
        uint64_t constant = getConstant(filter);
        
        std::cerr << relId << "." << colId << cmp << constant << " & ";
    }
    
    std::cerr << "|";
    
    // 打印选择项
    for (const auto& sel : selections) {
        std::cerr << getRelId(sel) << "." << getColId(sel) << " ";
    }
    std::cerr << "\n";
}

QueryInfo::~QueryInfo() {
    // 释放 estimations 中分配的 bitVector 内存
    for (auto& relStats : estimations) {
        for (auto& colStats : relStats) {
            if (colStats.bitVector != nullptr) {
                free(colStats.bitVector);
                colStats.bitVector = nullptr;
            }
        }
    }
}

} // namespace radix_join
