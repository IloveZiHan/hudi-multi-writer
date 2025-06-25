# Spark Kafka JSON消费者项目

本项目演示如何使用Spark Scala代码消费Kafka数据，并使用`from_json`函数解析Kafka中的JSON数据。

## 项目结构

```
spark-hudi-multi-writer/
├── src/main/scala/cn/com/multi_writer/
│   ├── util/
│   │   └── SparkSessionManager.scala      # SparkSession管理器
│   ├── KafkaStreamConsumer.scala          # Kafka流式消费者
│   └── KafkaBatchConsumer.scala           # Kafka批量消费者（演示版）
├── pom.xml                                # Maven依赖配置
└── README.md                              # 项目说明
```

## 主要功能

### 1. SparkSessionManager
- 统一管理SparkSession的创建和配置
- 预配置了Hudi、Kafka相关的Spark参数
- 支持本地和集群运行模式

### 2. KafkaStreamConsumer
- 实时消费Kafka数据流
- 使用`from_json`函数解析JSON数据
- 支持流式数据处理和实时打印

### 3. KafkaBatchConsumer
- 批量处理Kafka数据
- 演示JSON解析和数据分析功能
- 包含多种数据处理示例

## 数据格式

项目支持MySQL Binlog格式的JSON数据，包含以下字段：

```json
{
  "prefix": "VHmnZ",
  "logtype": "mysqlbinlog",
  "serverid": "102515364",
  "gtid": "cd0ea56d-ae52-11ef-8e8c-60db15ee09a7:4690001179",
  "eventtype": 30,
  "eventtypestr": "insert",
  "db": "alc",
  "table": "t_alc_loan_bal_dtl",
  "begintime": 1750523878,
  "column": ["`ID`", "`BIZ_DT`", "..."],
  "field": ["'6f651de793ac4a17a24632f91ade440f'", "'2025-06-22'", "..."],
  "orgoffset": 211353661230
}
```

## 使用方法

### 1. 编译项目

```bash
cd hudi-multi-writer
mvn clean compile
```

### 2. 运行批量消费示例

```bash
mvn exec:java -Dexec.mainClass="cn.com.multi_writer.KafkaBatchConsumer"
```

### 3. 运行流式消费

```bash
# 使用默认参数
mvn exec:java -Dexec.mainClass="cn.com.multi_writer.KafkaStreamConsumer"

# 或指定Kafka服务器和主题
mvn exec:java -Dexec.mainClass="cn.com.multi_writer.KafkaStreamConsumer" \
  -Dexec.args="localhost:9092 mysql-binlog-topic"
```

### 4. 打包运行

```bash
# 打包项目
mvn clean package

# 运行打包后的程序
java -cp target/spark-hudi-multi-writer-1.0.0-fat.jar \
  cn.com.multi_writer.KafkaBatchConsumer
```

## 核心功能演示

### JSON解析

```scala
// 定义JSON Schema
val binlogSchema = StructType(Array(
  StructField("db", StringType, nullable = true),
  StructField("table", StringType, nullable = true),
  StructField("eventtypestr", StringType, nullable = true),
  // ... 更多字段
))

// 使用from_json解析
val parsedDF = jsonDF
  .withColumn("parsed_data", from_json(col("json_value"), binlogSchema))
  .select("parsed_data.*")
```

### 数据处理示例

```scala
// 1. 按操作类型统计
parsedDF.groupBy("eventtypestr").count().show()

// 2. 使用SQL查询
parsedDF.createOrReplaceTempView("binlog_data")
spark.sql("SELECT db, table, COUNT(*) FROM binlog_data GROUP BY db, table").show()

// 3. 数组字段处理
parsedDF.select(explode(col("column")).as("column_name")).show()
```

## 依赖说明

主要依赖包括：
- Spark 3.5.0 (Core, SQL, Streaming, Kafka)
- Hudi 0.15.0
- Kafka Clients 2.8.2
- Scala 2.12.18
- FastJSON2 2.0.43

## 配置参数

### Spark配置
- `spark.master`: local[*] (本地运行)
- `spark.serializer`: KryoSerializer
- `spark.sql.adaptive.enabled`: true

### Kafka配置
- `kafka.bootstrap.servers`: localhost:9092
- `subscribe`: mysql-binlog-topic
- `startingOffsets`: latest

## 注意事项

1. 确保Kafka服务运行正常
2. 根据实际情况调整Kafka连接参数
3. JSON Schema需要与实际数据格式匹配
4. 流式处理需要足够的内存和CPU资源

## 故障排除

### 常见问题
1. **Kafka连接失败**: 检查Kafka服务状态和网络连接
2. **JSON解析错误**: 验证JSON格式和Schema定义
3. **内存不足**: 调整Spark内存配置参数

### 日志配置
项目已配置日志级别为WARN，减少输出信息。如需调试，可以修改SparkSessionManager中的日志级别设置。 