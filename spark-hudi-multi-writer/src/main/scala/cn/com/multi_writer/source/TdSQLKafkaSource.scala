package cn.com.multi_writer.source

import cn.com.multi_writer.schema.TdSQLKafkaSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.DataStreamReader

/**
 * Kafka数据源封装类
 * 负责消费Kafka数据并解析JSON格式的binlog消息
 */
class TdSQLKafkaSource(spark: SparkSession) {

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
        val kafkaTopic = conf.get("spark.kafka.topic", "rtdw_tdsql_alc")
        val startingOffsets = conf.get("spark.kafka.starting.offsets", "earliest")

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
        // 解析JSON数据，使用KafkaSchema中定义的schema
        kafkaStream
            .select(
                col("key").cast("string").as("kafka_key"),
                col("value").cast("string").as("kafka_value"),
                col("topic").as("kafka_topic"),
                col("partition").as("kafka_partition"),
                col("offset").as("kafka_offset"),
                col("timestamp").as("kafka_timestamp")
            )
            .withColumn("parsed_data", from_json(col("kafka_value"), TdSQLKafkaSchema.tdSQLBinlogSchema))
            .select(
                col("kafka_key"),
                col("kafka_value"),
                col("kafka_topic"),
                col("kafka_partition"),
                col("kafka_offset"),
                col("kafka_timestamp"),
                col("parsed_data.*")
            )
    }

    /**
     * 创建完整的解析后数据流
     * 包含创建Kafka流和解析消息的完整流程
     *
     * @return 解析后的结构化数据流
     */
    def createParsedStream(): DataFrame = {
        val kafkaStream = createKafkaStream()
        parseKafkaMessages(kafkaStream)
    }


} 