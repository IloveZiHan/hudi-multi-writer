#!/bin/bash

# MetaMySQLTableManager测试运行脚本
# 
# 用法：
#   ./run_tests.sh          # 运行所有测试
#   ./run_tests.sh -v       # 运行测试并显示详细输出
#   ./run_tests.sh -s       # 只运行MetaMySQLTableManager的测试
#   ./run_tests.sh -h       # 显示帮助信息

set -e

# 脚本目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 打印带颜色的消息
print_message() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
}

# 显示帮助信息
show_help() {
    echo "MetaMySQLTableManager测试运行脚本"
    echo ""
    echo "用法:"
    echo "  ./run_tests.sh [选项]"
    echo ""
    echo "选项:"
    echo "  -h, --help        显示此帮助信息"
    echo "  -v, --verbose     显示详细输出"
    echo "  -s, --specific    只运行MetaMySQLTableManager的测试"
    echo "  -c, --clean       清理之前的测试结果"
    echo "  -d, --debug       启用调试模式"
    echo ""
    echo "示例:"
    echo "  ./run_tests.sh                    # 运行所有测试"
    echo "  ./run_tests.sh -v                 # 运行测试并显示详细输出"
    echo "  ./run_tests.sh -s                 # 只运行MetaMySQLTableManager的测试"
    echo "  ./run_tests.sh -c -s              # 清理后运行特定测试"
    echo ""
}

# 检查Maven是否安装
check_maven() {
    if ! command -v mvn &> /dev/null; then
        print_message $RED "❌ Maven未安装或不在PATH中"
        exit 1
    fi
    
    print_message $GREEN "✅ Maven已安装: $(mvn --version | head -1)"
}

# 检查Java版本
check_java() {
    if ! command -v java &> /dev/null; then
        print_message $RED "❌ Java未安装或不在PATH中"
        exit 1
    fi
    
    local java_version=$(java -version 2>&1 | head -1 | cut -d'"' -f2)
    print_message $GREEN "✅ Java版本: $java_version"
}

# 清理之前的测试结果
clean_tests() {
    print_message $YELLOW "🧹 清理之前的测试结果..."
    mvn clean -q
    rm -rf logs/test-*.log 2>/dev/null || true
    print_message $GREEN "✅ 清理完成"
}

# 运行所有测试
run_all_tests() {
    print_message $BLUE "🚀 运行所有测试..."
    
    if [ "$VERBOSE" = true ]; then
        mvn test
    else
        mvn test -q
    fi
    
    print_message $GREEN "✅ 所有测试完成"
}

# 运行特定测试
run_specific_tests() {
    print_message $BLUE "🎯 运行MetaMySQLTableManager测试..."
    
    local test_opts="-Dtest=MetaMySQLTableManagerTest"
    
    if [ "$DEBUG" = true ]; then
        test_opts="$test_opts -Dmaven.surefire.debug=true"
    fi
    
    if [ "$VERBOSE" = true ]; then
        mvn test $test_opts
    else
        mvn test $test_opts -q
    fi
    
    print_message $GREEN "✅ MetaMySQLTableManager测试完成"
}

# 显示测试结果摘要
show_test_summary() {
    print_message $BLUE "📊 测试结果摘要:"
    
    if [ -f "target/surefire-reports/TEST-cn.com.multi_writer.meta.MetaMySQLTableManagerTest.xml" ]; then
        local xml_file="target/surefire-reports/TEST-cn.com.multi_writer.meta.MetaMySQLTableManagerTest.xml"
        local tests=$(grep -o 'tests="[^"]*"' "$xml_file" | cut -d'"' -f2)
        local failures=$(grep -o 'failures="[^"]*"' "$xml_file" | cut -d'"' -f2)
        local errors=$(grep -o 'errors="[^"]*"' "$xml_file" | cut -d'"' -f2)
        local time=$(grep -o 'time="[^"]*"' "$xml_file" | cut -d'"' -f2)
        
        echo "  总测试数: $tests"
        echo "  失败数: $failures"
        echo "  错误数: $errors"
        echo "  运行时间: ${time}s"
        
        if [ "$failures" = "0" ] && [ "$errors" = "0" ]; then
            print_message $GREEN "  状态: 全部通过 ✅"
        else
            print_message $RED "  状态: 有测试失败 ❌"
        fi
    else
        print_message $YELLOW "  测试报告文件未找到"
    fi
}

# 主函数
main() {
    # 解析命令行参数
    VERBOSE=false
    SPECIFIC=false
    CLEAN=false
    DEBUG=false
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                show_help
                exit 0
                ;;
            -v|--verbose)
                VERBOSE=true
                shift
                ;;
            -s|--specific)
                SPECIFIC=true
                shift
                ;;
            -c|--clean)
                CLEAN=true
                shift
                ;;
            -d|--debug)
                DEBUG=true
                shift
                ;;
            *)
                print_message $RED "❌ 未知选项: $1"
                show_help
                exit 1
                ;;
        esac
    done
    
    # 显示欢迎信息
    print_message $BLUE "🧪 MetaMySQLTableManager测试运行器"
    echo ""
    
    # 检查环境
    check_java
    check_maven
    echo ""
    
    # 清理（如果需要）
    if [ "$CLEAN" = true ]; then
        clean_tests
        echo ""
    fi
    
    # 运行测试
    if [ "$SPECIFIC" = true ]; then
        run_specific_tests
    else
        run_all_tests
    fi
    
    echo ""
    show_test_summary
    
    print_message $GREEN "🎉 测试运行完成！"
}

# 运行主函数
main "$@" 