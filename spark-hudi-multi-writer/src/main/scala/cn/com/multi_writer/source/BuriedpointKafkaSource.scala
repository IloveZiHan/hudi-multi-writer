package cn.com.multi_writer.source

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import cn.com.multi_writer.BuriedpointCdcStreamJob
import cn.com.multi_writer.schema.BuriedpointKafkaSchema

/**
 * 埋点数据Kafka数据源封装类
 * 负责消费Kafka数据并解析JSON格式的埋点消息
 */
class BuriedpointKafkaSource(spark: SparkSession) {

    private val logger: Logger = LoggerFactory.getLogger(getClass)

    // 导入隐式转换
    import spark.implicits._

    /**
     * 创建Kafka数据流
     * 从Spark配置中读取Kafka连接参数
     *
     * @return Kafka原始数据流
     */
    def createKafkaStream(): DataFrame = {
        val conf = spark.conf

        // 从Spark配置中获取Kafka参数
        val kafkaBrokers = conf.get("spark.kafka.bootstrap.servers", "10.94.162.31:9092")
        val kafkaTopic = conf.get("spark.kafka.topic", "tracker_marketing")
        val startingOffsets = conf.get("spark.kafka.starting.offsets", "earliest")

        logger.info(s"创建埋点数据Kafka流 - brokers: $kafkaBrokers, topic: $kafkaTopic, startingOffsets: $startingOffsets")

        // 创建Kafka数据流
        spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaBrokers)
            .option("subscribe", kafkaTopic)
            .option("startingOffsets", startingOffsets) // 从配置的offset位置开始消费
            .option("failOnDataLoss", "false") // 数据丢失时不失败
            .load()
    }

    /**
     * 解析Kafka消息，将JSON数据转换为结构化DataFrame
     *
     * @param kafkaStream 原始Kafka数据流
     * @return 解析后的结构化数据流
     */
    def parseKafkaMessages(kafkaStream: DataFrame): DataFrame = {
        logger.info("开始解析埋点数据Kafka消息...")
        
        // 获取预定义的埋点数据Schema
        val buriedpointSchema = BuriedpointKafkaSchema.buriedpointKafkaSchema
        
        // 解析JSON数据，使用预定义的埋点数据Schema
        val parsedDF = kafkaStream
            .select(
                col("key").cast("string").as("kafka_key"),
                col("value").cast("string").as("kafka_value"),
                col("topic").as("kafka_topic"),
                col("partition").as("kafka_partition"),
                col("offset").as("kafka_offset"),
                col("timestamp").as("kafka_timestamp")
            )
            // 过滤掉空值
            .filter(col("kafka_value").isNotNull && col("kafka_value") =!= "")
            // 将整个JSON数据转换为Map结构
            .withColumn("parsed_data", from_json(col("kafka_value"), buriedpointSchema))
            .filter(col("parsed_data").isNotNull)

        logger.info("埋点数据Kafka消息解析完成")
        parsedDF
    }

    /**
     * 创建完整的解析后数据流
     * 包含创建Kafka流和解析消息的完整流程
     *
     * @return 解析后的结构化数据流
     */
    def createParsedStream(): DataFrame = {
        logger.info("创建完整的埋点数据解析流...")
        val kafkaStream = createKafkaStream()
        val parsedStream = parseKafkaMessages(kafkaStream)
        
        logger.info("完整的埋点数据解析流创建完成")
        parsedStream
    }
} 