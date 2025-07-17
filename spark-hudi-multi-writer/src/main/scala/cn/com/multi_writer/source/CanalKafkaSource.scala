package cn.com.multi_writer.source

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import cn.com.multi_writer.SparkSessionManager
import cn.com.multi_writer.schema.CanalKafkaSchema

/**
 * Canal Kafka数据源
 * 用于从Kafka消费Canal binlog消息并解析为DataFrame
 */
class CanalKafkaSource(spark: SparkSession) {
    
    private val logger: Logger = LoggerFactory.getLogger(getClass)
    
    /**
     * 创建已解析的Canal数据流
     * @return 解析后的DataFrame流
     */
    def createParsedStream(): DataFrame = {
        logger.info("开始创建Canal Kafka解析流...")

        val conf = spark.conf

        // 从Spark配置中获取Kafka参数
        val kafkaBrokers = conf.get("spark.kafka.bootstrap.servers", "10.94.162.31:9092")
        val kafkaTopic = conf.get("spark.kafka.topic", "rtdw_mysql_sms")
        val startingOffsets = conf.get("spark.kafka.starting.offsets", "earliest")

        // 创建Kafka数据流
        val kafkaDF = spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaBrokers)
            .option("subscribe", kafkaTopic)
            .option("startingOffsets", startingOffsets) // 从配置的offset位置开始消费
            .option("failOnDataLoss", "false") // 数据丢失时不失败
            .load()
        
        // 使用Schema方式解析Canal消息
        val parsedDF = kafkaDF
            .select(
                col("topic"),
                col("partition"),
                col("offset"),
                col("timestamp"),
                col("key").cast(StringType).alias("message_key"),
                col("value").cast(StringType).alias("message_value")
            )
            .filter(col("message_value").isNotNull && col("message_value") =!= "")
            // 使用schema方式解析Canal JSON数据
            .withColumn("canal_data", from_json(col("message_value"), CanalKafkaSchema.canalBinlogSchema))
            // 从解析后的结构中提取字段
            .select(
                col("topic"),
                col("partition"),
                col("offset"),
                col("timestamp"),
                col("message_key"),
                col("message_value"),
                col("canal_data.database").alias("database"),
                col("canal_data.table").alias("table"),
                col("canal_data.type").alias("event_type"),
                col("canal_data.ts").alias("binlog_timestamp"),
                col("canal_data.es").alias("event_timestamp"),
                col("canal_data.isDdl").alias("is_ddl"),
                col("canal_data.gtid").alias("gtid"),
                col("canal_data.id").alias("record_id"),
                col("canal_data.sql").alias("sql"),
                col("canal_data.pkNames").alias("pk_names"),
                col("canal_data.mysqlType").alias("mysql_type"),
                col("canal_data.sqlType").alias("sql_type"),
                col("canal_data.data").alias("data"),
                col("canal_data.old").alias("old")
            )
            // 数据有效性验证和默认值处理
            .withColumn("is_valid", 
                when(col("database").isNotNull && col("table").isNotNull && col("event_type").isNotNull, true)
                .otherwise(false)
            )
            // 处理null值，设置默认值
            .withColumn("database", coalesce(col("database"), lit("")))
            .withColumn("table", coalesce(col("table"), lit("")))
            .withColumn("event_type", coalesce(col("event_type"), lit("")))
            .withColumn("binlog_timestamp", coalesce(col("binlog_timestamp"), lit(0L)))
            .withColumn("event_timestamp", coalesce(col("event_timestamp"), lit(0L)))
            .withColumn("is_ddl", coalesce(col("is_ddl"), lit(false)))
            .withColumn("gtid", coalesce(col("gtid"), lit("")))
            .withColumn("record_id", coalesce(col("record_id"), lit(0)))
            .withColumn("sql", coalesce(col("sql"), lit("")))
            .withColumn("pk_names", coalesce(col("pk_names"), array()))
            .withColumn("mysql_type", coalesce(col("mysql_type"), map()))
            .withColumn("sql_type", coalesce(col("sql_type"), map()))
            .withColumn("data", coalesce(col("data"), array()))
            .withColumn("old", coalesce(col("old"), array()))
            // 为了兼容现有代码，添加原有字段名映射
            .withColumn("data_json", to_json(col("data")))
            .withColumn("old_json", to_json(col("old")))
            .withColumn("pk_names_str", to_json(col("pk_names")))
            .withColumn("mysql_type_str", to_json(col("mysql_type")))
            .withColumn("sql_type_str", to_json(col("sql_type")))
            // 只保留有效的数据
            .filter(col("is_valid") === true)
        
        logger.info("Canal Kafka解析流创建完成")
        parsedDF
    }
    
    /**
     * 创建带有扩展解析的数据流
     * 包含更详细的字段解析和数据清洗
     */
    def createExtendedParsedStream(): DataFrame = {
        logger.info("开始创建扩展Canal Kafka解析流...")
        
        val basicDF = createParsedStream()
        
        // 进一步解析和处理数据
        val extendedDF = basicDF
            // 添加数据变更时间（使用当前时间作为处理时间）
            .withColumn("process_time", current_timestamp())
            // 计算数据行数
            .withColumn("data_count", 
                when(col("data").isNotNull, size(col("data")))
                .otherwise(0)
            )
            // 根据事件类型添加操作标识
            .withColumn("operation", 
                when(col("event_type") === "INSERT", "I")
                .when(col("event_type") === "UPDATE", "U")
                .when(col("event_type") === "DELETE", "D")
                .otherwise("O")
            )
            // 添加分区键（用于后续写入分区）
            .withColumn("partition_key", concat(col("database"), lit("."), col("table")))
            // 添加主键数量统计
            .withColumn("pk_count", 
                when(col("pk_names").isNotNull, size(col("pk_names")))
                .otherwise(0)
            )
            // 添加字段类型统计
            .withColumn("mysql_type_count", 
                when(col("mysql_type").isNotNull, size(col("mysql_type")))
                .otherwise(0)
            )
            // 添加数据完整性标识
            .withColumn("has_data", col("data_count") > 0)
            .withColumn("has_old_data", 
                when(col("old").isNotNull, size(col("old")) > 0)
                .otherwise(false)
            )
        
        logger.info("扩展Canal Kafka解析流创建完成")
        extendedDF
    }
    
    /**
     * 获取指定表的解析流
     * @param database 数据库名
     * @param table 表名
     * @return 过滤后的DataFrame流
     */
    def getTableStream(database: String, table: String): DataFrame = {
        logger.info(s"创建表 $database.$table 的专用解析流...")
        
        createExtendedParsedStream()
            .filter(col("database") === database && col("table") === table)
    }
    
    /**
     * 获取指定数据库的所有表的解析流
     * @param database 数据库名
     * @return 过滤后的DataFrame流
     */
    def getDatabaseStream(database: String): DataFrame = {
        logger.info(s"创建数据库 $database 的解析流...")
        
        createExtendedParsedStream()
            .filter(col("database") === database)
    }
    
    /**
     * 获取指定事件类型的解析流
     * @param eventTypes 事件类型列表（INSERT, UPDATE, DELETE）
     * @return 过滤后的DataFrame流
     */
    def getEventTypeStream(eventTypes: String*): DataFrame = {
        logger.info(s"创建事件类型 ${eventTypes.mkString(",")} 的解析流...")
        
        createExtendedParsedStream()
            .filter(col("event_type").isin(eventTypes: _*))
    }
    
    /**
     * 获取非DDL操作的解析流
     * @return 过滤后的DataFrame流
     */
    def getNonDDLStream(): DataFrame = {
        logger.info("创建非DDL操作的解析流...")
        
        createExtendedParsedStream()
            .filter(col("is_ddl") === false)
    }
}