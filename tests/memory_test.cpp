#include <iostream>
#include <vector>

#include "Optimizer.hpp"

int main() {
    std::cout << "Running memory test..." << std::endl;
    
    radix_join::ColumnStats stat;
    std::vector<uint64_t> testColumn = {1, 2, 3, 4, 5};
    
    // 调用findStats函数
    radix_join::findStats(testColumn.data(), &stat, testColumn.size());
    
    std::cout << "Min: " << stat.minValue << std::endl;
    std::cout << "Max: " << stat.maxValue << std::endl;
    std::cout << "Discrete values: " << stat.discreteValues << std::endl;
    
    // 释放内存
    if (stat.bitVector) {
        delete[] stat.bitVector;
        stat.bitVector = nullptr;
        std::cout << "Memory released successfully" << std::endl;
    }
    
    std::cout << "Test completed!" << std::endl;
    return 0;
}