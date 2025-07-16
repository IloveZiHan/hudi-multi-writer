package cn.com.multi_writer.schema

import org.apache.spark.sql.types._

/**
 * OGG Kafka消息Schema定义
 * 包含OGG Binlog JSON数据的结构定义
 */
object OggKafkaSchema {
  
  /**
   * OGG Binlog JSON数据的Schema定义
   * 用于解析从Kafka接收到的OGG binlog消息
   */
  val oggBinlogSchema: StructType = StructType(Array(
    StructField("table", StringType, nullable = true),            // 表名（格式：SCHEMA.TABLE）
    StructField("op_type", StringType, nullable = true),          // 操作类型（I/U/D）
    StructField("op_ts", StringType, nullable = true),            // 操作时间戳
    StructField("current_ts", StringType, nullable = true),       // 当前时间戳
    StructField("pos", StringType, nullable = true),              // 位置信息
    StructField("primary_keys", ArrayType(StringType), nullable = true),      // 主键列名数组
    StructField("before", MapType(StringType, StringType), nullable = true),  // 更新前的数据
    StructField("after", MapType(StringType, StringType), nullable = true) // 更新后的数据（字段名到值的映射）
  ))
} 