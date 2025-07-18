package cn.com.multi_writer

import org.apache.spark.sql.SparkSession
import scala.collection.mutable
import org.slf4j.{Logger, LoggerFactory}

/**
 * SparkSession管理器，用于统一管理SparkSession的创建和配置
 * 用于集中管理Spark相关的配置项，便于统一维护
 */
object SparkSessionManager {

  // 创建logger实例
  private val logger: Logger = LoggerFactory.getLogger(SparkSessionManager.getClass)

  /**
   * 根据当前执行的主类名称获取对应的Kafka topic
   * @return 对应的Kafka topic名称
   */
  private def getKafkaTopicByMainClass(): String = {
    try {
      // 获取当前线程的堆栈信息
      val stackTrace = Thread.currentThread().getStackTrace()
      
      // 查找main方法所在的类
      val mainClassOption = stackTrace.find(_.getMethodName == "main")
        .map(_.getClassName)
      
      mainClassOption match {
        case Some(className) if className.contains("OggCdcStreamJob") =>
          logger.info(s"检测到主类: $className，设置Kafka topic为: realtime_dataware")
          "realtime_dataware"
        case Some(className) if className.contains("TdsqlCdcStreamJob") =>
          logger.info(s"检测到主类: $className，设置Kafka topic为: rtdw_tdsql_alc")
          "rtdw_tdsql_alc"
        case Some(className) if className.contains("CanalCdcStreamJob") =>
          logger.warn(s"未识别的主类: $className，设置Kafka topic为: rtdw_mysql_sms")
          "rtdw_mysql_sms"
        case Some(className) if className.contains("BuriedpointCdcStreamJob") =>
          logger.warn(s"未识别的主类: $className，设置Kafka topic为: tracker_marketing")
          "tracker_marketing"
        case None =>
          logger.warn("无法检测到主类，使用默认Kafka topic: rtdw_tdsql_alc")
          "rtdw_tdsql_alc"
      }
    } catch {
      case e: Exception =>
        logger.error(s"检测主类时发生异常: ${e.getMessage}, 使用默认Kafka topic: rtdw_tdsql_alc")
        "rtdw_tdsql_alc"
    }
  }

  /**
   * 根据当前执行的主类名称获取对应的应用程序名称
   * @return 对应的应用程序名称
   */
  private def getApplicationNameByMainClass(): String = {
    try {
      // 获取当前线程的堆栈信息
      val stackTrace = Thread.currentThread().getStackTrace()
      
      // 查找main方法所在的类
      val mainClassOption = stackTrace.find(_.getMethodName == "main")
        .map(_.getClassName)
      
      mainClassOption match {
        case Some(className) if className.contains("OggCdcStreamJob") =>
          logger.info(s"检测到主类: $className，设置应用程序名称为: spark-hudi-cdc-oracle-stream-job")
          "spark-hudi-cdc-oracle-stream-job"
        case Some(className) if className.contains("TdsqlCdcStreamJob") =>
          logger.info(s"检测到主类: $className，设置应用程序名称为: spark-hudi-tdsql-stream-job")
          "spark-hudi-tdsql-stream-job"
        case Some(className) if className.contains("CanalCdcStreamJob$") =>
          logger.warn(s"未识别的主类: $className，使用默认应用程序名称: spark-hudi-cdc-canal-stream-job")
          "spark-hudi-cdc-canal-stream-job"
        case Some(className) if className.contains("BuriedpointCdcStreamJob") =>
          logger.warn(s"未识别的主类: $className，使用默认应用程序名称: spark-hudi-cdc-bp-stream-job")
          "spark-hudi-cdc-bp-stream-job"
        case None =>
          logger.warn("无法检测到主类，使用默认应用程序名称: spark-hudi-cdc-stream-job")
          "spark-hudi-cdc-stream-job"
      }
    } catch {
      case e: Exception =>
        logger.error(s"检测主类时发生异常: ${e.getMessage}, 使用默认应用程序名称: spark-hudi-cdc-stream-job")
        "spark-hudi-cdc-stream-job"
    }
  }

  // 定义系统默认配置值，用于判断用户是否修改了配置
  private val DEFAULT_CONFIGS = Map(
    "spark.application.name" -> getApplicationNameByMainClass(), // 根据主类动态设置应用程序名称
    "spark.kafka.bootstrap.servers" -> "10.94.162.31:9092",
    "spark.kafka.topic" -> getKafkaTopicByMainClass(), // 根据主类动态设置topic
    "spark.shuffle_partition.records" -> "100000",
    "spark.kafka.consumer.group.id" -> "hudi-cdc-consumer-group",
    "spark.kafka.auto.offset.reset" -> "earliest",
    "spark.kafka.starting.offsets" -> "earliest",
    "spark.sql.streaming.trigger.processingTime" -> "5 seconds",
    "spark.sql.streaming.checkpointLocation" -> "/tmp/spark-checkpoint",
    "spark.meta.table.path" -> "/tmp/spark-warehouse/meta_hudi_table",
    "spark.sql.warehouse.dir" -> "file:///tmp/spark-warehouse",
    "spark.hive.metastore.uris" -> "thrift://huoshan-test04:9083,thrift://huoshan-test03:9083,thrift://huoshan-test05:9083",
    "spark.mysql.url" -> "jdbc:mysql://10.94.158.109:33061/rtdw_meta",
    "spark.mysql.user" -> "root",
    "spark.mysql.password" -> "H5*ajAXVhT"
  )

  /**
   * 创建并配置SparkSession
   * @param appName 应用名称
   * @return 配置好的SparkSession实例
   */
  def createSparkSession(appName: String): SparkSession = {
    val spark = SparkSession.builder()
        .appName(appName)
        .master("local[*]") // 本地运行模式，使用所有可用核心
        // Spark基础配置
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        // Hudi相关配置
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        // 仓库目录配置
        .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
        // UI配置
        .config("spark.ui.enabled", "true")
        .config("spark.ui.port", "4040")
        // 禁用Hudi元数据表功能，简化配置
        .config("spark.hive.metastore.uris", "thrift://huoshan-test04:9083,thrift://huoshan-test03:9083,thrift://huoshan-test05:9083")
        .config("hoodie.table.metadata.enable", "false")
        .config("hoodie.metadata.enable", "false")
        // 设置SQL严格模式为false，提高兼容性
        .config("spark.sql.strict", "false")
        // Kafka相关配置
        .config("spark.sql.streaming.kafka.useDeprecatedOffsetFetching", "false")
        .getOrCreate()

    // 配置Hadoop文件系统
    spark.sparkContext.hadoopConfiguration.set("fs.defaultFS", "file:///")

    // 设置日志级别
    spark.sparkContext.setLogLevel("WARN")

    spark
  }

  /**
   * 获取Kafka和流处理相关的配置参数
   * 统一管理配置参数获取，避免在各个地方重复代码
   * 
   * @param spark SparkSession实例
   * @return 配置参数的Map
   */
  def getStreamingConfigs(spark: SparkSession): Map[String, String] = {
    Map(
      "applicationName" -> spark.conf.get("spark.application.name", DEFAULT_CONFIGS("spark.application.name")),
      "kafkaBrokers" -> spark.conf.get("spark.kafka.bootstrap.servers", DEFAULT_CONFIGS("spark.kafka.bootstrap.servers")),
      "kafkaTopic" -> spark.conf.get("spark.kafka.topic", DEFAULT_CONFIGS("spark.kafka.topic")),
      "recordsPerPartition" -> spark.conf.get("spark.shuffle_partition.records", DEFAULT_CONFIGS("spark.shuffle_partition.records")),
      "consumerGroupId" -> spark.conf.get("spark.kafka.consumer.group.id", DEFAULT_CONFIGS("spark.kafka.consumer.group.id")),
      "offsetReset" -> spark.conf.get("spark.kafka.auto.offset.reset", DEFAULT_CONFIGS("spark.kafka.auto.offset.reset")),
      "startingOffsets" -> spark.conf.get("spark.kafka.starting.offsets", DEFAULT_CONFIGS("spark.kafka.starting.offsets")),
      "processingTime" -> spark.conf.get("spark.sql.streaming.trigger.processingTime", DEFAULT_CONFIGS("spark.sql.streaming.trigger.processingTime")),
      "checkpointLocation" -> spark.conf.get("spark.sql.streaming.checkpointLocation", DEFAULT_CONFIGS("spark.sql.streaming.checkpointLocation")),
      "metaTablePath" -> spark.conf.get("spark.meta.table.path", DEFAULT_CONFIGS("spark.meta.table.path")),
      "hudiBasePath" -> spark.conf.get("spark.sql.warehouse.dir",DEFAULT_CONFIGS("spark.sql.warehouse.dir")),
      "hmsServerAddress" -> spark.conf.get("spark.hive.metastore.uris",DEFAULT_CONFIGS("spark.hive.metastore.uris")),
      "mysqlUrl" -> spark.conf.get("spark.mysql.url",DEFAULT_CONFIGS("spark.mysql.url")),
      "mysqlUser" -> spark.conf.get("spark.mysql.user",DEFAULT_CONFIGS("spark.mysql.user")),
      "mysqlPassword" -> spark.conf.get("spark.mysql.password",DEFAULT_CONFIGS("spark.mysql.password")) 
    )
  }

  /**
   * 打印用户自定义的配置参数（非默认值的配置）
   * 只显示用户通过spark-submit或代码显式设置的配置项
   * 
   * @param spark SparkSession实例
   */
  def printCustomConfigs(spark: SparkSession): Unit = {
    logger.info("=== 用户自定义配置参数 ===")
    
    val customConfigs = mutable.ListBuffer[(String, String, String)]()
    
    // 检查每个默认配置项是否被用户修改
    DEFAULT_CONFIGS.foreach { case (key, defaultValue) =>
      try {
        val currentValue = spark.conf.get(key, defaultValue)
        if (currentValue != defaultValue) {
          customConfigs += ((key, currentValue, defaultValue))
        }
      } catch {
        case _: Exception => // 忽略获取配置时的异常
      }
    }
    
    // 获取所有用户设置的配置，过滤出相关的配置项
    val allConfigs = spark.conf.getAll
    val relevantPrefixes = Set(
      "spark.kafka.",
      "spark.shuffle_partition.",
      "spark.sql.streaming.",
      "hoodie.",
      "spark.sql.adaptive.",
      "spark.serializer"
    )
    
    allConfigs.foreach { case (key, value) =>
      // 如果配置项以相关前缀开头，且不在默认配置列表中，则认为是用户自定义的
      if (relevantPrefixes.exists(prefix => key.startsWith(prefix)) && 
          !DEFAULT_CONFIGS.contains(key)) {
        customConfigs += ((key, value, "未设置"))
      }
    }
    
    if (customConfigs.nonEmpty) {
      customConfigs.sortBy(_._1).foreach { case (key, currentValue, defaultValue) =>
        logger.info(f"  $key%-50s : $currentValue (默认值: $defaultValue)")
      }
    } else {
      logger.info("  未检测到用户自定义配置，全部使用默认值")
    }
    
    logger.info("=" * 50)
  }

  /**
   * 打印流处理相关配置的详细信息
   * 
   * @param spark SparkSession实例
   */
  def printStreamingConfigDetails(spark: SparkSession): Unit = {
    logger.info("=== 流处理配置详情 ===")
    
    // 仅输出DEFAULT_CONFIGS中的参数
    DEFAULT_CONFIGS.foreach { case (key, defaultValue) =>
      val currentValue = spark.conf.get(key, defaultValue)
      logger.info(f"  $key: $currentValue")
    }
    
    logger.info("=" * 50)
  }

  /**
   * 停止SparkSession
   * @param spark SparkSession实例
   */
  def stopSparkSession(spark: SparkSession): Unit = {
    if (spark != null && !spark.sparkContext.isStopped) {
      spark.stop()
    }
  }
} 