package cn.hnzycfc.hudi

import cn.com.multi_writer.SparkSessionManager
import cn.com.multi_writer.source.KafkaSource
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * TdSQL CDC 流处理作业
 *
 * 优化后的版本特性：
 * 1. 所有配置参数通过Spark配置传入，而不是命令行参数
 * 2. Kafka消费逻辑封装在KafkaSource中
 * 3. Binlog schema定义分离到KafkaSchema中
 * 4. 数据解析逻辑封装在KafkaSource中
 *
 * 配置参数说明：
 * - spark.kafka.bootstrap.servers: Kafka服务器地址 (默认: localhost:9092)
 * - spark.kafka.topic: Kafka主题名称 (默认: default-topic)
 * - spark.kafka.partition.count: 数据分区数 (默认: 4)
 *
 * 使用示例:
 * spark-submit \
 *   --conf spark.kafka.bootstrap.servers=10.94.162.31:9092 \
 *   --conf spark.kafka.topic=rtdw_tdsql_alc \
 *   --conf spark.kafka.partition.count=8 \
 *   your-jar-file.jar
 */
object TdsqlCdcStreamJob {

  def main(args: Array[String]): Unit = {
    // 使用SparkSessionManager创建SparkSession
    val spark = SparkSessionManager.createSparkSession("TdSQL CDC Stream Job")

    try {
      println("=== TdSQL CDC 流处理作业启动 ===")

      // 从Spark配置中读取参数
      val kafkaBrokers = spark.conf.get("spark.kafka.bootstrap.servers", "localhost:9092")
      val kafkaTopic = spark.conf.get("spark.kafka.topic", "default-topic")
      val partitionCount = spark.conf.get("spark.kafka.partition.count", "4").toInt

      println("配置参数:")
      println(s"  Kafka Brokers: $kafkaBrokers")
      println(s"  Kafka Topic: $kafkaTopic")
      println(s"  数据分区数: $partitionCount")

      // 创建KafkaSource实例
      val kafkaSource = new KafkaSource(spark)

      // 创建解析后的数据流
      val parsedStream = kafkaSource.createParsedStream()

      println("Kafka数据流创建成功，开始启动流处理...")

      // 使用foreachBatch进行微批处理
      val query = parsedStream.writeStream
          .outputMode("append")
          .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
            processBatch(spark, batchDF, batchId, partitionCount, kafkaSource)
          }
          .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("5 seconds")) // 5秒触发一次
          .start()

      println("流处理已启动，使用foreachBatch进行微批处理...")
      println("按 Ctrl+C 停止程序")

      // 等待流处理完成
      query.awaitTermination()

    } catch {
      case e: Exception =>
        println(s"程序执行出错: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      // 停止SparkSession
      SparkSessionManager.stopSparkSession(spark)
      println("程序执行完成")
    }
  }

  /**
   * 处理微批次数据
   *
   * @param spark SparkSession实例
   * @param batchDF 批次DataFrame
   * @param batchId 批次ID
   * @param partitionCount 数据分区数
   * @param kafkaSource KafkaSource实例，用于数据清洗
   */
  def processBatch(spark: SparkSession, batchDF: DataFrame, batchId: Long,
                   partitionCount: Int, kafkaSource: KafkaSource): Unit = {
    println(s"========== 处理批次 $batchId ==========")

    // 统计总数据量
    val totalCount = batchDF.count()
    println(s"总数据量: $totalCount 条")

    // 设置Job组，在统计完数据量后设置，展示数据行数
    spark.sparkContext.setJobGroup(s"batch_$batchId",
      s"Processing Kafka batch $batchId with $totalCount records", interruptOnCancel = true)

    if (totalCount > 0) {
      // 使用KafkaSource中的数据清洗方法
      val cleanedDF = kafkaSource.cleanData(batchDF)

      // 对清洗后的数据进行shuffle和cache处理
      val shuffledAndCachedDF = shuffleAndCacheData(cleanedDF, partitionCount)

      // 这里可以添加更多数据处理逻辑
      processCleanedData(spark, cleanedDF)

      // 释放缓存（可选）
      // shuffledAndCachedDF.unpersist()
    } else {
      println("本批次无数据")
    }

    println(s"========================================")
    println()
  }

  /**
   * 对数据进行shuffle和cache处理
   *
   * @param df 待处理的DataFrame
   * @param partitionCount 目标分区数
   * @return shuffle和cache后的DataFrame
   */
  def shuffleAndCacheData(df: DataFrame, partitionCount: Int): DataFrame = {
    println(s"开始进行数据shuffle和cache处理...")
    println(s"原始分区数: ${df.rdd.partitions.length}")
    println(s"目标分区数: $partitionCount")

    // 获取原始数据量，用于决定shuffle策略
    val originalCount = df.count()
    println(s"原始数据量: $originalCount 条")

    // 使用repartition进行shuffle，确保数据均匀分布
    val repartitionedDF = if (originalCount > 10000) {
      println("数据量较大，使用带随机键的repartition策略")
      // 对于大数据量，添加随机列来改善分区均匀性
      df.withColumn("__shuffle_key__", (rand() * partitionCount).cast("int"))
          .repartition(partitionCount, col("__shuffle_key__"))
          .drop("__shuffle_key__")
    } else {
      println("数据量较小，使用标准repartition策略")
      // 对于小数据量，直接使用repartition
      df.repartition(partitionCount)
    }

    // 使用persist将数据缓存到内存和磁盘中，提高后续访问性能
    val cachedDF = repartitionedDF.persist(StorageLevel.MEMORY_AND_DISK_SER)

    // 触发一次action操作来实际执行repartition和cache
    val recordCount = cachedDF.count()
    println(s"shuffle和cache处理完成，记录数: $recordCount")

    cachedDF
  }

  /**
   * 处理清洗后的数据
   * 可以在这里添加具体的业务逻辑
   *
   * @param spark SparkSession实例
   * @param df 清洗后的DataFrame
   */
  def processCleanedData(spark: SparkSession, df: DataFrame): Unit = {
    println("开始处理清洗后的数据...")

    // 示例：统计不同事件类型的数量
    println("事件类型统计:")
    df.groupBy("eventtypestr")
        .count()
        .orderBy(desc("count"))
        .show(10, truncate = false)

    // 示例：统计不同数据库的操作
    println("数据库操作统计:")
    df.filter(col("db").isNotNull)
        .groupBy("db", "eventtypestr")
        .count()
        .show(20)

    // 示例：显示最近的一些记录
    println("最近处理的记录样例:")
    df.select("db", "table", "eventtypestr", "begintime", "column", "field")
        .filter(col("db").isNotNull && col("table").isNotNull)
        .orderBy(desc("begintime"))
        .show(5, truncate = false)

    println("数据处理完成")
  }
} 