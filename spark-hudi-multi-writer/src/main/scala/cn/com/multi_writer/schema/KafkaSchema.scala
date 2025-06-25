package cn.com.multi_writer.schema

import org.apache.spark.sql.types._

/**
 * Kafka消息Schema定义
 * 包含TdSQL Binlog JSON数据的结构定义
 */
object KafkaSchema {
  
  /**
   * TdSQL Binlog JSON数据的Schema定义
   * 用于解析从Kafka接收到的binlog消息
   */
  val binlogSchema: StructType = StructType(Array(
    StructField("prefix", StringType, nullable = true),           // 前缀标识
    StructField("logtype", StringType, nullable = true),          // 日志类型
    StructField("localport", IntegerType, nullable = true),       // 本地端口
    StructField("localip", StringType, nullable = true),          // 本地IP
    StructField("filename", StringType, nullable = true),         // 文件名
    StructField("gtid_flag2", StringType, nullable = true),       // GTID标志2
    StructField("serverid", StringType, nullable = true),         // 服务器ID
    StructField("gtid", StringType, nullable = true),             // 全局事务ID
    StructField("event_index", StringType, nullable = true),      // 事件索引
    StructField("sequence_num", LongType, nullable = true),       // 序列号
    StructField("eventtype", IntegerType, nullable = true),       // 事件类型编码
    StructField("eventtypestr", StringType, nullable = true),     // 事件类型字符串
    StructField("db", StringType, nullable = true),               // 数据库名
    StructField("table", StringType, nullable = true),            // 表名
    StructField("begintime", LongType, nullable = true),          // 开始时间戳
    StructField("gtid_commitid", StringType, nullable = true),    // GTID提交ID
    StructField("column", ArrayType(StringType), nullable = true), // 列名数组
    StructField("where", ArrayType(StringType), nullable = true),  // WHERE条件数组
    StructField("field", ArrayType(StringType), nullable = true),  // 字段值数组
    StructField("sub_event_index", StringType, nullable = true),   // 子事件索引
    StructField("orgoffset", LongType, nullable = true)           // 原始偏移量
  ))
} 