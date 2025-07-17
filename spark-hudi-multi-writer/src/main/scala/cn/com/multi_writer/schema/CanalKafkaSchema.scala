package cn.com.multi_writer.schema

import org.apache.spark.sql.types._

/**
 * Canal Kafka消息Schema定义
 * 包含Canal Binlog JSON数据的完整结构定义
 */
object CanalKafkaSchema {
  
  /**
   * Canal Binlog JSON数据的Schema定义
   * 用于解析从Kafka接收到的Canal binlog消息
   * 基于实际的Canal JSON数据格式定义
   */
  val canalBinlogSchema: StructType = StructType(Array(
    StructField("database", StringType, nullable = true),                    // 数据库名
    StructField("table", StringType, nullable = true),                       // 表名
    StructField("type", StringType, nullable = true),                        // 操作类型：INSERT/UPDATE/DELETE
    StructField("ts", LongType, nullable = true),                           // 时间戳
    StructField("es", LongType, nullable = true),                           // 事件时间戳
    StructField("isDdl", BooleanType, nullable = true),                     // 是否为DDL操作
    StructField("gtid", StringType, nullable = true),                       // 全局事务ID
    StructField("id", IntegerType, nullable = true),                        // 记录ID
    StructField("sql", StringType, nullable = true),                        // SQL语句
    StructField("pkNames", ArrayType(StringType), nullable = true),         // 主键字段名数组
    StructField("mysqlType", MapType(StringType, StringType), nullable = true), // MySQL字段类型映射
    StructField("sqlType", MapType(StringType, IntegerType), nullable = true),  // SQL字段类型映射（JDBC类型码）
    StructField("data", ArrayType(MapType(StringType, StringType)), nullable = true), // 数据数组
    StructField("old", ArrayType(MapType(StringType, StringType)), nullable = true)   // 旧数据数组（UPDATE操作时）
  ))

  /**
   * 解析后的Canal数据的扩展Schema定义
   * 包含处理过程中添加的附加字段
   */
  val canalParsedSchema: StructType = StructType(Array(
    // Kafka元数据字段
    StructField("kafka_topic", StringType, nullable = true),
    StructField("kafka_partition", IntegerType, nullable = true),
    StructField("kafka_offset", LongType, nullable = true),
    StructField("kafka_timestamp", LongType, nullable = true),
    StructField("message_key", StringType, nullable = true),
    StructField("message_value", StringType, nullable = true),
    
    // Canal原始字段
    StructField("database", StringType, nullable = true),
    StructField("table", StringType, nullable = true),
    StructField("event_type", StringType, nullable = true),
    StructField("binlog_timestamp", LongType, nullable = true),
    StructField("event_timestamp", LongType, nullable = true),
    StructField("is_ddl", BooleanType, nullable = true),
    StructField("gtid", StringType, nullable = true),
    StructField("record_id", IntegerType, nullable = true),
    StructField("sql", StringType, nullable = true),
    StructField("pk_names", ArrayType(StringType), nullable = true),
    StructField("mysql_type", MapType(StringType, StringType), nullable = true),
    StructField("sql_type", MapType(StringType, IntegerType), nullable = true),
    StructField("data", ArrayType(MapType(StringType, StringType)), nullable = true),
    StructField("old", ArrayType(MapType(StringType, StringType)), nullable = true),
    
    // 处理过程中添加的字段
    StructField("process_time", TimestampType, nullable = true),
    StructField("data_count", IntegerType, nullable = true),
    StructField("operation", StringType, nullable = true),
    StructField("partition_key", StringType, nullable = true),
    StructField("is_valid", BooleanType, nullable = true)
  ))
} 