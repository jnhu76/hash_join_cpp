#include <iostream>
#include <string>

#include "Joiner.hpp"
#include "Parser.hpp"
#include "JobScheduler.hpp"
#include "Optimizer.hpp"

using namespace radix_join;

/**
 * @brief 主函数 - 基数哈希连接查询处理器入口
 * 
 * 程序流程：
 * 1. 创建 Joiner 对象，从标准输入加载所有关系数据
 * 2. 创建 JobScheduler，启动工作线程池
 * 3. 循环读取查询，解析并执行
 * 4. 清理资源并退出
 */
int main(int argc, char const* argv[])
{
    // 创建 Joiner 对象
    // Joiner 持有所有关系的元数据
    Joiner joiner;

    // 从标准输入读取关系文件名列表，加载所有关系
    // 并根据数据大小设置 RADIX_BITS 和 initSize
    std::cerr << "[DEBUG] Setting up joiner..." << std::endl;
    joiner.setup();
    std::cerr << "[DEBUG] Joiner setup complete. Relations: " << joiner.getNumOfRelations() << std::endl;

    // 创建 JobScheduler（工作线程、任务队列等）
    std::cerr << "[DEBUG] Creating job scheduler..." << std::endl;
    createJobScheduler(&js);
    std::cerr << "[DEBUG] Job scheduler created." << std::endl;

    // 读取查询行，解析并执行
    // 流程：解析查询 -> 创建统计估计 -> 应用过滤器/连接估计 -> 执行连接 -> 输出校验和
    std::string line;
    while (std::getline(std::cin, line)) {
        // 跳过结束标记行
        if (line == "F") continue;

        std::cerr << "[DEBUG] Processing query: " << line << std::endl;

        // 解析查询字符串
        QueryInfo query(line);
        std::cerr << "[DEBUG] Query parsed." << std::endl;

        // 从 Joiner 复制统计信息用于查询优化
        std::cerr << "[DEBUG] Creating query estimations..." << std::endl;
        query.createQueryEstimations(joiner);
        std::cerr << "[DEBUG] Query estimations created." << std::endl;

        // 应用列等值估计（同一关系内的列比较）
        std::cerr << "[DEBUG] Applying column equality estimations..." << std::endl;
        applyColEqualityEstimations(query, joiner);
        std::cerr << "[DEBUG] Applying filter estimations..." << std::endl;
        // 应用过滤器估计
        applyFilterEstimations(query, joiner);
        // 应用连接估计
        applyJoinEstimations(query, joiner);

        // 查找最优连接顺序（当前被注释掉）
        // findOptimalJoinOrder(query, joiner);

        // 执行连接查询并输出校验和
        joiner.join(query);
    }

    // 清理资源：停止工作线程，释放任务队列
    destroyJobScheduler(js);

    return 0;
}
