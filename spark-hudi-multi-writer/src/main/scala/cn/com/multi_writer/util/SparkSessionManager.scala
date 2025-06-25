package cn.com.multi_writer

import org.apache.spark.sql.SparkSession

/**
 * SparkSession管理器，用于统一管理SparkSession的创建和配置
 * 用于集中管理Spark相关的配置项，便于统一维护
 */
object SparkSessionManager {

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
   * 停止SparkSession
   * @param spark SparkSession实例
   */
  def stopSparkSession(spark: SparkSession): Unit = {
    if (spark != null && !spark.sparkContext.isStopped) {
      spark.stop()
    }
  }
} 