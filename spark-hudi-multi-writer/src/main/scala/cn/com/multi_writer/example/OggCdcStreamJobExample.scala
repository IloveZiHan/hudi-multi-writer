package cn.com.multi_writer.example

import cn.com.multi_writer.{OggCdcStreamJob, SparkSessionManager}
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

/**
 * OggCdcStreamJob使用示例
 * 
 * 这个示例展示了如何配置和运行OGG CDC Stream Job
 */
object OggCdcStreamJobExample {
  
  private val logger: Logger = LoggerFactory.getLogger(OggCdcStreamJobExample.getClass)
  
  def main(args: Array[String]): Unit = {
    
    logger.info("=== OGG CDC Stream Job 示例启动 ===")
    
    // 创建SparkSession并设置OGG相关配置
    val spark = SparkSession.builder()
      .appName("OGG CDC Stream Job Example")
      .master("local[*]")
      // Kafka配置
      .config("spark.kafka.bootstrap.servers", "localhost:9092")
      .config("spark.kafka.topic", "ogg_binlog_topic")
      .config("spark.kafka.starting.offsets", "earliest")
      .config("spark.kafka.consumer.group.id", "ogg-cdc-consumer-group")
      // MySQL元数据配置
      .config("spark.mysql.url", "jdbc:mysql://localhost:3306/meta_db")
      .config("spark.mysql.user", "root")
      .config("spark.mysql.password", "password")
      // Hudi并发配置
      .config("spark.hudi.concurrent.max", "3")
      .config("spark.hudi.concurrent.timeout", "600")
      .config("spark.hudi.concurrent.failfast", "true")
      // 其他配置
      .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
      .config("spark.sql.streaming.checkpointLocation", "/tmp/ogg-checkpoint")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .getOrCreate()
    
    try {
      logger.info("Spark配置完成，开始启动OGG CDC Stream Job...")
      
      // 显示配置信息
      SparkSessionManager.printStreamingConfigDetails(spark)
      
      // 调用OggCdcStreamJob的main方法
      // 注意：这里传入空的args数组，因为配置已经在SparkSession中设置
      OggCdcStreamJob.main(Array.empty[String])
      
    } catch {
      case e: Exception =>
        logger.error(s"OGG CDC Stream Job示例运行失败: ${e.getMessage}", e)
        throw e
    } finally {
      spark.stop()
      logger.info("OGG CDC Stream Job示例完成")
    }
  }
  
  /**
   * 创建测试用的OGG消息JSON示例
   */
  def createTestOggMessage(): String = {
    """
    {
      "table": "UCENTER.MT_USER_BASE_UNREGISTER",
      "op_type": "I",
      "op_ts": "2025-07-16 09:24:43.000220",
      "current_ts": "2025-07-16T09:24:49.096001",
      "pos": "00000001960444572183",
      "primary_keys": ["ID"],
      "after": {
        "ID": "553f039916914ca59af42f1426d187f4",
        "MOBILE": "15995285210",
        "USER_NAME": null,
        "USER_SEX": null,
        "ID_TYPE": "20",
        "ID_NO": null,
        "CHANNEL": "Android",
        "APP_LOGINED": "1",
        "STATE": "0",
        "MARKET_CHANNEL": "ZYXF",
        "CREATE_TIME": "2025-07-16 09:23:43.378000000",
        "UPDATE_TIME": "2025-07-16 09:24:43.877000000",
        "MOVE_TIME": "2025-07-16 09:24:43.877000000",
        "TYPE": "02",
        "ACCOUNT_FLAG": "ZYXF"
      }
    }
    """
  }
  
  /**
   * 打印OGG消息处理的关键步骤
   */
  def printProcessingSteps(): Unit = {
    logger.info("=== OGG消息处理步骤 ===")
    logger.info("1. 从Kafka读取OGG binlog消息")
    logger.info("2. 解析JSON格式的OGG消息")
    logger.info("3. 标准化操作类型（I/U/D -> INSERT/UPDATE/DELETE）")
    logger.info("4. 解析表名（SCHEMA.TABLE -> schema + table）")
    logger.info("5. 从after字段中提取数据")
    logger.info("6. 根据元数据表配置进行字段映射和类型转换")
    logger.info("7. 添加CDC字段（cdc_dt, cdc_delete_flag）")
    logger.info("8. 并发写入多个Hudi表")
    logger.info("========================")
  }
} 