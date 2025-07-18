#!/bin/bash

# Canal CDC Stream Job 运行脚本
# 功能：编译并运行Canal数据发送到Kafka的功能

echo "=========================================="
echo "Canal CDC Stream Job 运行脚本"
echo "=========================================="

# 设置项目目录
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "项目目录: $PROJECT_DIR"

# 进入项目目录
cd "$PROJECT_DIR"

# 检查必要文件是否存在
if [ ! -f "src/main/resources/canal_data.json" ]; then
    echo "错误: 找不到canal_data.json文件"
    exit 1
fi

if [ ! -f "src/main/scala/cn/com/multi_writer/CanalCdcStreamJob.scala" ]; then
    echo "错误: 找不到CanalCdcStreamJob.scala文件"
    exit 1
fi

# 清理和编译项目
echo "正在清理和编译项目..."
mvn clean compile -q

if [ $? -ne 0 ]; then
    echo "编译失败，请检查代码"
    exit 1
fi

echo "编译成功"

# 运行Canal CDC Stream Job
echo "正在运行Canal CDC Stream Job..."
echo "目标Kafka Topic: rtdw_mysql_sms"
echo "Kafka服务器: 10.94.162.31:9092"
echo "=========================================="

# 使用scala-maven-plugin执行
mvn scala:run -Dexec.mainClass="cn.com.multi_writer.CanalCdcStreamJob" -Dexec.args="" -q

if [ $? -eq 0 ]; then
    echo "=========================================="
    echo "Canal CDC Stream Job 执行成功"
    echo "=========================================="
else
    echo "=========================================="
    echo "Canal CDC Stream Job 执行失败"
    echo "=========================================="
    exit 1
fi 