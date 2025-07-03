package cn.com.multi_writer.meta

import org.apache.spark.sql.SparkSession
import org.junit.{After, Before}
import java.io.File
import java.nio.file.{Files, Paths}
import scala.util.Random
import org.slf4j.{Logger, LoggerFactory}

/**
 * 测试基类
 * 
 * 提供统一的测试环境设置，简化SparkSession管理
 */
trait TestBase {

  var spark: SparkSession = _
  var tempDir: String = _
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * 创建简化的SparkSession用于测试
   */
  def createTestSparkSession(appName: String): SparkSession = {
    try {
      SparkSession.builder()
        .appName(appName)
        .master("local[2]")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.warehouse.dir", s"file:///$tempDir/")
        .config("spark.sql.adaptive.enabled", "false") // 简化配置
        .config("spark.ui.enabled", "false") // 禁用UI避免端口冲突
        .config("spark.driver.host", "localhost")
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .config("hoodie.table.metadata.enable", "false")
        .config("hoodie.metadata.enable", "false")
        .getOrCreate()
    } catch {
      case e: Exception =>
        logger.warn(s"创建SparkSession失败，尝试更简化的配置: ${e.getMessage}")
        // 如果上面的配置失败，尝试最简化的配置
        SparkSession.builder()
          .appName(appName)
          .master("local[1]")
          .config("spark.ui.enabled", "false")
          .config("spark.sql.warehouse.dir", s"file:///$tempDir/")
          .getOrCreate()
    }
  }

  /**
   * 测试前的初始化
   */
  @Before
  def setUpBase(): Unit = {
    tempDir = s"/tmp/spark-warehouse"
    Files.createDirectories(Paths.get(tempDir))
    
    // 创建SparkSession
    spark = createTestSparkSession("Test Base")
    spark.sparkContext.setLogLevel("ERROR") // 减少日志输出
    
    logger.info(s"=== 测试环境初始化完成 ===")
    logger.info(s"临时目录: $tempDir")
    logger.info(s"Spark版本: ${spark.version}")
  }

  /**
   * 测试后的清理
   */
  @After
  def tearDownBase(): Unit = {
    try {
      if (spark != null && !spark.sparkContext.isStopped) {
        spark.stop()
      }
    } finally {
      logger.info("=== 测试环境清理完成 ===")
    }
  }
} 