set_project("radix-hash-join")
set_version("1.0.0")
set_languages("c++20")

-- 添加标准编译模式规则
add_rules("mode.debug", "mode.release")

-- 1. 内存检测模式 (mode.check) - AddressSanitizer
add_rules("mode.check")

-- 2. 性能分析模式 (mode.profile) - gprof
add_rules("mode.profile")

-- 3. 覆盖分析模式 (mode.coverage) - gcov
add_rules("mode.coverage")

-- 4. Valgrind内存分析模式 (mode.valgrind)
add_rules("mode.valgrind")

-- 5. AddressSanitizer模式 (mode.asan)
add_rules("mode.asan")

-- 6. ThreadSanitizer模式 (mode.tsan)
add_rules("mode.tsan")

-- 7. LeakSanitizer模式 (mode.lsan)
add_rules("mode.lsan")


-- 主目标 Driver
target("Driver")
    set_kind("binary")
    add_includedirs("include")
    add_files("src/*.cpp")
    -- 排除 harness.cpp（单独的目标）
    remove_files("src/harness.cpp")
    add_cxxflags("-Wall", "-Wextra")
    add_syslinks("pthread")
    
    -- 调试模式配置
    if is_mode("debug") then
        set_symbols("debug")
        set_optimize("none")
        add_cxxflags("-g", "-fno-omit-frame-pointer")
    end
    
    -- 发布模式配置
    if is_mode("release") then
        set_symbols("hidden")
        set_optimize("fastest")
        add_cxxflags("-O3")
    end
    
    -- 内存检测模式 (check) - AddressSanitizer
    if is_mode("check") then
        set_symbols("debug")
        set_optimize("none")
        add_cxflags("-fsanitize=address", "-ftrapv")
        add_mxflags("-fsanitize=address", "-ftrapv")
        add_ldflags("-fsanitize=address")
    end
    
    -- 性能分析模式 (profile) - gprof
    if is_mode("profile") then
        set_symbols("debug")
        add_cxflags("-pg")
        add_ldflags("-pg")
    end
    
    -- 覆盖分析模式 (coverage) - gcov
    if is_mode("coverage") then
        add_cxflags("--coverage")
        add_mxflags("--coverage")
        add_ldflags("--coverage")
    end
    
    -- Valgrind内存分析模式 (valgrind)
    if is_mode("valgrind") then
        set_symbols("debug")
        set_optimize("none")
        -- 为Valgrind优化的编译选项
        add_cxxflags("-g", "-fno-omit-frame-pointer", "-fno-inline", "-O0")
        -- 禁用一些可能干扰Valgrind的优化
        add_cxxflags("-fno-builtin")
    end
    
    -- AddressSanitizer模式 (asan)
    if is_mode("asan") then
        set_symbols("debug")
        set_optimize("none")
        add_cxflags("-fsanitize=address")
        add_ldflags("-fsanitize=address")
    end
    
    -- ThreadSanitizer模式 (tsan)
    if is_mode("tsan") then
        set_symbols("debug")
        set_optimize("none")
        add_cxflags("-fsanitize=thread")
        add_ldflags("-fsanitize=thread")
    end
    
    -- LeakSanitizer模式 (lsan)
    if is_mode("lsan") then
        set_symbols("debug")
        set_optimize("none")
        add_cxflags("-fsanitize=leak")
        add_ldflags("-fsanitize=leak")
    end
    
    -- UndefinedBehaviorSanitizer模式 (ubsan)
    if is_mode("ubsan") then
        set_symbols("debug")
        set_optimize("none")
        add_cxflags("-fsanitize=undefined")
        add_ldflags("-fsanitize=undefined")
    end

-- harness 目标 - 测试框架
target("harness")
    set_kind("binary")
    add_includedirs("include")
    add_files("src/harness.cpp")
    add_cxxflags("-O3", "-Wall", "-Wextra")
    set_languages("c++20")

-- 测试目标
target("optimizer_test")
    set_kind("binary")
    add_includedirs("include")
    add_files("tests/optimizer_test.cpp", "src/Optimizer.cpp")
    add_cxxflags("-O3", "-Wall", "-Wextra")
    set_languages("c++20")
    
    -- 调试模式配置
    if is_mode("debug") then
        set_symbols("debug")
        set_optimize("none")
        add_cxxflags("-g", "-fno-omit-frame-pointer")
    end
    
    -- Valgrind模式配置
    if is_mode("valgrind") then
        set_symbols("debug")
        set_optimize("none")
        add_cxxflags("-g", "-fno-omit-frame-pointer", "-fno-inline", "-O0")
    end

-- Valgrind运行任务 - 直接在xmake中运行valgrind
target("valgrind_run")
    set_kind("phony")
    set_default(false)
    
    on_run(function ()
        import("lib.detect.find_tool")
        
        -- 检查 valgrind 是否可用
        local valgrind = find_tool("valgrind")
        if not valgrind then
            raise("valgrind not found, please install it first (sudo apt install valgrind)")
        end
        
        -- 确定二进制文件路径
        local binary = "build/linux/x86_64/valgrind/Driver"
        if not os.isfile(binary) then
            -- 尝试其他模式
            binary = "build/linux/x86_64/debug/Driver"
        end
        if not os.isfile(binary) then
            binary = "build/linux/x86_64/release/Driver"
        end
        
        if not os.isfile(binary) then
            raise("Driver binary not found. Please build first:\n" ..
                  "  xmake f -m valgrind && xmake build\n" ..
                  "or:\n" ..
                  "  xmake f -m debug && xmake build")
        end
        
        -- 创建报告目录
        os.mkdir("valgrind_reports")
        
        -- 生成带时间戳的日志文件名
        local timestamp = os.date("%Y%m%d_%H%M%S")
        local log_file = string.format("valgrind_reports/valgrind_%s.log", timestamp)
        
        print("========================================")
        print("Running Valgrind Memory Check")
        print("========================================")
        print(string.format("Binary: %s", binary))
        print(string.format("Log file: %s", log_file))
        print("")
        
        -- 运行 valgrind
        os.execv(valgrind.program, {
            "--leak-check=full",
            "--show-leak-kinds=all",
            "--track-origins=yes",
            "--verbose",
            "--log-file=" .. log_file,
            binary
        })
        
        print("")
        print("========================================")
        print(string.format("Valgrind report saved to: %s", log_file))
        print("========================================")
    end)

-- Valgrind运行workload任务
target("valgrind_workload")
    set_kind("phony")
    set_default(false)
    
    on_run(function ()
        import("lib.detect.find_tool")
        
        -- 检查 valgrind 是否可用
        local valgrind = find_tool("valgrind")
        if not valgrind then
            raise("valgrind not found, please install it first (sudo apt install valgrind)")
        end
        
        -- 确定二进制文件路径
        local binary = "build/linux/x86_64/valgrind/Driver"
        if not os.isfile(binary) then
            binary = "build/linux/x86_64/debug/Driver"
        end
        if not os.isfile(binary) then
            binary = "build/linux/x86_64/release/Driver"
        end
        
        if not os.isfile(binary) then
            raise("Driver binary not found. Please build first:\n" ..
                  "  xmake f -m valgrind && xmake build")
        end
        
        -- workload 路径
        local workload_dir = "workloads/small"
        local init_file = workload_dir .. "/small.init"
        local work_file = workload_dir .. "/small.work"
        
        if not os.isfile(init_file) or not os.isfile(work_file) then
            raise("Workload files not found in " .. workload_dir)
        end
        
        -- 创建报告目录
        os.mkdir("valgrind_reports")
        
        -- 生成带时间戳的日志文件名
        local timestamp = os.date("%Y%m%d_%H%M%S")
        local log_file = string.format("valgrind_reports/workload_%s.log", timestamp)
        
        print("========================================")
        print("Running Valgrind with Workload")
        print("========================================")
        print(string.format("Binary: %s", binary))
        print(string.format("Workload: %s", workload_dir))
        print(string.format("Log file: %s", log_file))
        print("")
        
        -- 构建输入并运行 valgrind
        local input_cmd = string.format("cat %s && echo 'Done' && cat %s", init_file, work_file)
        local full_cmd = string.format("%s | %s --leak-check=full --show-leak-kinds=all --track-origins=yes --log-file=%s %s",
            input_cmd, valgrind.program, log_file, binary)
        
        os.exec("bash", {"-c", full_cmd})
        
        print("")
        print("========================================")
        print(string.format("Valgrind report saved to: %s", log_file))
        print("========================================")
    end)

-- 深度内存检测任务
target("memcheck")
    set_kind("phony")
    set_default(false)
    
    on_run(function ()
        import("lib.detect.find_tool")
        
        local valgrind = find_tool("valgrind")
        if not valgrind then
            raise("valgrind not found, please install it first")
        end
        
        local binary = "build/linux/x86_64/valgrind/Driver"
        if not os.isfile(binary) then
            binary = "build/linux/x86_64/debug/Driver"
        end
        if not os.isfile(binary) then
            raise("Driver binary not found. Please build first: xmake f -m valgrind && xmake build")
        end
        
        os.mkdir("valgrind_reports")
        local timestamp = os.date("%Y%m%d_%H%M%S")
        local log_file = string.format("valgrind_reports/memcheck_%s.log", timestamp)
        
        print("Running deep memory check...")
        print(string.format("Binary: %s", binary))
        print(string.format("Log file: %s", log_file))
        
        os.execv(valgrind.program, {
            "--leak-check=full",
            "--show-leak-kinds=all",
            "--track-origins=yes",
            "--expensive-definedness-checks=yes",
            "--keep-stacktraces=alloc-and-free",
            "--verbose",
            "--log-file=" .. log_file,
            binary
        })
        
        print(string.format("\nMemory check report saved to: %s", log_file))
    end)

-- 清理任务 - 删除所有生成的目录和文件
after_clean(function ()
    print("Cleaning generated files...")
    
    -- 删除 build 目录
    if os.isdir("build") then
        os.rm("build")
        print("  Removed: build/")
    end
    
    -- 删除 .xmake 目录
    if os.isdir(".xmake") then
        os.rm(".xmake")
        print("  Removed: .xmake/")
    end
    
    -- 删除 valgrind_reports 目录
    if os.isdir("valgrind_reports") then
        os.rm("valgrind_reports")
        print("  Removed: valgrind_reports/")
    end
    
    -- 删除 verification 报告
    if os.isdir("verification/reports") then
        os.rm("verification/reports")
        print("  Removed: verification/reports/")
    end
    
    -- 删除 compile_commands.json
    if os.isfile("compile_commands.json") then
        os.rm("compile_commands.json")
        print("  Removed: compile_commands.json")
    end
    
    -- 删除 .cache 目录
    if os.isdir(".cache") then
        os.rm(".cache")
        print("  Removed: .cache/")
    end
    
    -- 删除 coverage 文件
    for _, file in ipairs(os.files("*.gcov") or {}) do
        os.rm(file)
        print(string.format("  Removed: %s", file))
    end
    for _, file in ipairs(os.files("*.gcda") or {}) do
        os.rm(file)
        print(string.format("  Removed: %s", file))
    end
    for _, file in ipairs(os.files("*.gcno") or {}) do
        os.rm(file)
        print(string.format("  Removed: %s", file))
    end
    
    -- 删除 gmon.out (gprof)
    if os.isfile("gmon.out") then
        os.rm("gmon.out")
        print("  Removed: gmon.out")
    end
    
    print("\n✅ Clean complete!")
end)
