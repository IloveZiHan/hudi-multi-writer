# OGG CDC Stream Job 使用说明

## 概述

OggCdcStreamJob是一个基于Spark Streaming的流处理作业，用于消费Kafka中的OGG（Oracle GoldenGate）binlog消息，并将其写入Apache Hudi表。

## 功能特性

1. **OGG消息解析**：支持解析OGG的JSON格式binlog消息
2. **操作类型转换**：自动将OGG操作类型（I/U/D）转换为标准格式（INSERT/UPDATE/DELETE）
3. **表名解析**：从"SCHEMA.TABLE"格式中提取schema和table信息
4. **并发写入**：使用HudiConcurrentWriter实现多表并发写入
5. **动态表配置**：支持从元数据表动态获取表配置信息

## OGG消息格式

OGG消息格式如下：
```json
{
  "table": "UCENTER.MT_USER_BASE_UNREGISTER",
  "op_type": "I",
  "op_ts": "2025-07-16 09:24:43.000220",
  "current_ts": "2025-07-16T09:24:49.096001",
  "pos": "00000001960444572183",
  "primary_keys": ["ID"],
  "after": {
    "ID": "553f039916914ca59af42f1426d187f4",
    "MOBILE": "15995285210",
    "USER_NAME": null,
    "CHANNEL": "Android",
    "CREATE_TIME": "2025-07-16 09:23:43.378000000",
    "UPDATE_TIME": "2025-07-16 09:24:43.877000000"
  }
}
```

## 核心组件

### 1. OggKafkaSchema
定义OGG消息的Spark Schema结构：
- `table`: 表名（SCHEMA.TABLE格式）
- `op_type`: 操作类型（I/U/D）
- `op_ts`: 操作时间戳
- `after`: 数据字段的Map结构

### 2. OggBatchProcessor
负责OGG消息的数据预处理：
- **standardizeEventType**: 将OGG操作类型转换为标准格式
- **parseTableName**: 解析表名，分离schema和table
- **extractAndConvertFields**: 从after字段中提取数据并进行类型转换
- **batchTransform**: 批量转换多个表的数据

### 3. OggKafkaSource
负责从Kafka读取OGG消息：
- **createKafkaStream**: 创建Kafka数据流
- **parseKafkaMessages**: 解析OGG JSON消息
- **createParsedStream**: 创建完整的解析后数据流

### 4. OggCdcStreamJob
主要的流处理作业：
- **processBatchOptimized**: 优化的批次处理函数
- **transformationWithConcurrentWriter**: 使用并发写入器进行数据转换
- **getDynamicTableConfigs**: 获取动态表配置

## 使用方法

### 1. 配置参数

在提交Spark作业时，需要配置以下参数：

```bash
spark-submit \
  --class cn.com.multi_writer.OggCdcStreamJob \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.kafka.bootstrap.servers=localhost:9092 \
  --conf spark.kafka.topic=ogg_binlog_topic \
  --conf spark.kafka.starting.offsets=earliest \
  --conf spark.hudi.concurrent.max=3 \
  --conf spark.hudi.concurrent.timeout=600 \
  --conf spark.mysql.url=jdbc:mysql://localhost:3306/meta_db \
  --conf spark.mysql.user=root \
  --conf spark.mysql.password=password \
  your-jar-file.jar
```

### 2. 元数据表配置

需要在MySQL元数据表中配置表信息：
- `source_db`: 源数据库名（对应OGG消息中的schema）
- `source_table`: 源表名（对应OGG消息中的table）
- `schema`: 表的字段定义（JSON格式）
- `partition_expr`: 分区表达式
- `hoodie_config`: Hudi配置选项

### 3. 运行作业

```bash
# 启动OGG CDC Stream Job
spark-submit \
  --class cn.com.multi_writer.OggCdcStreamJob \
  --master yarn \
  --deploy-mode cluster \
  your-jar-file.jar
```

## 主要差异对比

| 特性 | TdSQL CDC | OGG CDC |
|------|-----------|---------|
| 消息格式 | 包含field/where数组 | 包含after对象 |
| 操作类型 | eventtypestr字段 | op_type字段（I/U/D） |
| 表名格式 | 分离的db和table字段 | 组合的table字段（SCHEMA.TABLE） |
| 数据提取 | 从field/where数组中提取 | 从after对象中提取 |
| 字段映射 | 使用column数组映射 | 直接使用字段名 |

## 性能优化

1. **全局并发写入器**：使用全局HudiConcurrentWriter实例，避免频繁创建/销毁线程池
2. **数据缓存**：使用DataFrame缓存提高重复访问效率
3. **并发处理**：支持多表并发写入，提高吞吐量
4. **资源管理**：自动清理遗留任务，优化内存使用

## 监控和调试

1. **日志输出**：详细的日志记录，包括数据处理过程和写入结果
2. **性能统计**：记录批次处理时间、写入时间等性能指标
3. **错误处理**：完善的异常处理和错误恢复机制

## 注意事项

1. 确保Kafka topic中的OGG消息格式正确
2. 正确配置元数据表中的表信息
3. 根据数据量调整并发写入器的配置参数
4. 定期清理Hudi表的旧数据以节省存储空间 