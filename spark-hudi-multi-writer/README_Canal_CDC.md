# Canal CDC Stream Job 使用说明

## 概述

Canal CDC Stream Job 是一个基于Apache Spark的实时数据处理作业，用于消费Canal产生的MySQL binlog消息并将其写入Apache Hudi表中。该作业参考了`TdsqlCdcStreamJob`的实现，专门针对Canal的消息格式进行了优化。

## 特性

- **实时CDC处理**: 消费Canal产生的MySQL binlog消息
- **并发写入**: 使用全局并发写入器，支持多表同时写入Hudi
- **动态表配置**: 从元数据表动态获取表配置信息
- **数据类型转换**: 自动将MySQL数据类型转换为Spark数据类型
- **错误处理**: 完善的错误处理和日志记录机制
- **检查点支持**: 支持流处理检查点，确保数据不丢失

## 核心组件

### 1. CanalKafkaSource
- 从Kafka消费Canal binlog消息
- 解析Canal JSON格式消息
- 提取数据库、表名、操作类型等元数据信息

### 2. CanalBatchProcessor
- 处理Canal binlog消息的批次数据
- 将JSON格式的数据转换为DataFrame
- 支持INSERT、UPDATE、DELETE操作类型
- 进行数据类型转换和字段映射

### 3. CanalCdcStreamJob
- 主要的流处理作业类
- 管理全局并发写入器
- 协调各个组件的工作
- 提供流处理的生命周期管理

## Canal消息格式

Canal产生的binlog消息格式如下：

```json
{
  "data": [
    {
      "field1": "value1",
      "field2": "value2",
      ...
    }
  ],
  "database": "database_name",
  "table": "table_name",
  "type": "INSERT|UPDATE|DELETE",
  "ts": 1672531200000,
  "isDdl": false,
  "pkNames": ["id"],
  "mysqlType": {
    "field1": "varchar(255)",
    "field2": "bigint(20)"
  },
  "sqlType": {
    "field1": 12,
    "field2": -5
  },
  "old": [
    {
      "field1": "old_value1",
      "field2": "old_value2"
    }
  ]
}
```

## 配置说明

### Spark配置

在`spark-defaults.conf`或提交作业时设置以下配置：

```properties
# Kafka配置
spark.streaming.kafka.bootstrap.servers=localhost:9092
spark.streaming.kafka.topics=canal_topic
spark.streaming.kafka.group.id=canal_cdc_group
spark.streaming.kafka.starting.offsets=latest

# MySQL元数据库配置
spark.streaming.mysql.url=jdbc:mysql://localhost:3306/metadata
spark.streaming.mysql.user=root
spark.streaming.mysql.password=password

# Hudi配置
spark.streaming.hudi.base.path=/path/to/hudi/tables
spark.hudi.concurrent.max=3
spark.hudi.concurrent.timeout=600
spark.hudi.concurrent.failfast=true

# 应用配置
spark.streaming.application.name=canal_cdc_app
spark.streaming.checkpoint.location=/path/to/checkpoint
```

### 元数据表结构

需要在MySQL中创建表配置元数据表：

```sql
CREATE TABLE table_configs (
    id VARCHAR(100) PRIMARY KEY,
    schema TEXT NOT NULL,
    status BOOLEAN DEFAULT TRUE,
    is_partitioned BOOLEAN DEFAULT FALSE,
    partition_expr VARCHAR(500),
    hoodie_config TEXT,
    tags VARCHAR(200),
    description VARCHAR(500),
    source_db VARCHAR(100),
    source_table VARCHAR(100),
    db_type VARCHAR(50),
    application_name VARCHAR(100),
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
```

### 表配置示例

```sql
INSERT INTO table_configs (
    id, 
    schema, 
    status, 
    is_partitioned, 
    partition_expr,
    hoodie_config,
    tags,
    description, 
    source_db, 
    source_table, 
    db_type,
    application_name
) VALUES (
    'user_info_hudi',
    '{
        "type": "struct",
        "fields": [
            {"name": "user_id", "type": "long", "nullable": false},
            {"name": "username", "type": "string", "nullable": true},
            {"name": "email", "type": "string", "nullable": true},
            {"name": "created_time", "type": "timestamp", "nullable": true}
        ]
    }',
    TRUE,
    TRUE,
    'to_date(created_time)',
    '{
        "hoodie.table.name": "user_info_hudi",
        "hoodie.datasource.write.recordkey.field": "user_id",
        "hoodie.datasource.write.precombine.field": "created_time",
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": "upsert"
    }',
    'user,profile',
    '用户信息表',
    'user_db',
    'user_info',
    'mysql',
    'canal_cdc_app'
);
```

## 运行方式

### 1. 提交Spark作业

```bash
spark-submit \
  --class cn.com.multi_writer.CanalCdcStreamJob \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 4 \
  --executor-memory 2g \
  --executor-cores 2 \
  --driver-memory 1g \
  --conf spark.streaming.kafka.bootstrap.servers=localhost:9092 \
  --conf spark.streaming.kafka.topics=canal_topic \
  --conf spark.streaming.kafka.group.id=canal_cdc_group \
  --conf spark.streaming.mysql.url=jdbc:mysql://localhost:3306/metadata \
  --conf spark.streaming.mysql.user=root \
  --conf spark.streaming.mysql.password=password \
  --conf spark.streaming.hudi.base.path=/path/to/hudi/tables \
  --conf spark.streaming.application.name=canal_cdc_app \
  --conf spark.streaming.checkpoint.location=/path/to/checkpoint \
  spark-hudi-multi-writer-1.0.jar
```

### 2. 本地调试运行

```bash
# 设置环境变量
export SPARK_HOME=/path/to/spark
export JAVA_HOME=/path/to/java

# 运行作业
$SPARK_HOME/bin/spark-submit \
  --class cn.com.multi_writer.CanalCdcStreamJob \
  --master local[*] \
  --conf spark.streaming.kafka.bootstrap.servers=localhost:9092 \
  --conf spark.streaming.kafka.topics=canal_topic \
  --conf spark.streaming.kafka.group.id=canal_cdc_group \
  --conf spark.streaming.mysql.url=jdbc:mysql://localhost:3306/metadata \
  --conf spark.streaming.mysql.user=root \
  --conf spark.streaming.mysql.password=password \
  --conf spark.streaming.hudi.base.path=/tmp/hudi/tables \
  --conf spark.streaming.application.name=canal_cdc_app \
  --conf spark.streaming.checkpoint.location=/tmp/checkpoint \
  target/spark-hudi-multi-writer-1.0.jar
```

## 监控和调优

### 1. 日志监控

作业会输出详细的日志信息，包括：
- Canal消息解析状态
- 表配置加载情况
- 数据转换结果
- Hudi写入状态
- 性能统计信息

### 2. 性能调优

- **并发度调整**: 通过`spark.hudi.concurrent.max`调整并发写入数量
- **批处理间隔**: 调整`ProcessingTime`间隔，平衡延迟和吞吐量
- **内存配置**: 根据数据量调整executor内存和并行度
- **检查点频率**: 设置合适的检查点位置和频率

### 3. 监控指标

- 消息消费速率
- 数据转换成功率
- Hudi写入成功率
- 端到端延迟
- 错误率和重试次数

## 故障处理

### 1. 常见问题

- **Kafka连接问题**: 检查Kafka服务状态和网络连接
- **MySQL连接问题**: 检查数据库连接配置和权限
- **Hudi写入失败**: 检查Hudi表配置和HDFS权限
- **数据格式错误**: 检查Canal消息格式和表结构配置

### 2. 恢复策略

- **检查点恢复**: 从最近的检查点恢复流处理状态
- **重新提交作业**: 在修复配置后重新提交作业
- **数据回溯**: 通过调整Kafka offset进行数据回溯处理

## 注意事项

1. **数据一致性**: Canal CDC保证最终一致性，但不保证严格的事务一致性
2. **资源管理**: 合理配置Spark资源，避免OOM和性能问题
3. **表结构变更**: DDL变更需要同步更新元数据表配置
4. **监控告警**: 建议配置监控告警机制，及时发现和处理问题
5. **数据重复**: 在异常情况下可能出现数据重复，需要在下游处理时考虑幂等性

## 扩展功能

该作业还可以扩展以下功能：
- 支持更多数据源类型（如PostgreSQL、Oracle等）
- 集成数据质量检查
- 支持数据血缘追踪
- 增加自定义数据转换逻辑
- 支持多种输出格式（Delta Lake、Iceberg等） 