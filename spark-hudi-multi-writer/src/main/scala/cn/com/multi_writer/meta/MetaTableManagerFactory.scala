package cn.com.multi_writer.meta

import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

/**
 * 元数据表管理器工厂类
 *
 * 提供创建不同类型的MetaTableManager实例的工厂方法
 */
object MetaTableManagerFactory {
  
  private val logger: Logger = LoggerFactory.getLogger(MetaTableManagerFactory.getClass)

  /**
   * 存储类型枚举
   */
  object StorageType extends Enumeration {
    val HUDI, MYSQL = Value
  }

  /**
   * 创建MySQL版本的MetaTableManager
   *
   * @param spark SparkSession实例
   * @param mysqlUrl MySQL数据库连接URL
   * @param mysqlUser MySQL数据库用户名
   * @param mysqlPassword MySQL数据库密码
   * @return MetaTableManager实例
   */
  def createMySQLMetaTableManager(spark: SparkSession,
                                  mysqlUrl: String,
                                  mysqlUser: String,
                                  mysqlPassword: String): MetaTableManager = {
    logger.info("创建MySQL版本的MetaTableManager")
    new MetaMySQLTableManager(spark, mysqlUrl, mysqlUser, mysqlPassword)
  }

  /**
   * 根据存储类型创建MetaTableManager
   *
   * @param storageType 存储类型
   * @param spark SparkSession实例
   * @param config 配置参数Map
   * @return MetaTableManager实例
   */
  def createMetaTableManager(storageType: StorageType.Value,
                            spark: SparkSession,
                            config: Map[String, String] = Map.empty): MetaTableManager = {
    
    storageType match {
      case StorageType.MYSQL =>
        logger.info("创建MySQL版本的MetaTableManager")
        val mysqlUrl = config.getOrElse("mysql.url", 
          throw new IllegalArgumentException("MySQL URL is required"))
        val mysqlUser = config.getOrElse("mysql.user", 
          throw new IllegalArgumentException("MySQL user is required"))
        val mysqlPassword = config.getOrElse("mysql.password", 
          throw new IllegalArgumentException("MySQL password is required"))
        
        new MetaMySQLTableManager(spark, mysqlUrl, mysqlUser, mysqlPassword)
    }
  }

  /**
   * 从SparkSession配置中读取配置并创建MetaTableManager
   *
   * @param spark SparkSession实例
   * @param storageType 存储类型，如果为空则从配置中读取
   * @return MetaTableManager实例
   */
  def createFromSparkConfig(spark: SparkSession,
                           storageType: StorageType.Value = null): MetaTableManager = {
    
    // 从SparkSession配置中读取存储类型
    val actualStorageType = Option(storageType).getOrElse {
      val configType = spark.conf.getOption("meta.storage.type").getOrElse("hudi")
      configType.toLowerCase match {
        case "hudi" => StorageType.HUDI
        case "mysql" => StorageType.MYSQL
        case _ => 
          logger.warn(s"未知的存储类型: $configType, 使用默认的Hudi存储")
          StorageType.HUDI
      }
    }
    
    actualStorageType match {
      case StorageType.MYSQL =>
        val mysqlUrl = spark.conf.getOption("meta.mysql.url")
          .getOrElse(throw new IllegalArgumentException("配置项 meta.mysql.url 是必需的"))
        val mysqlUser = spark.conf.getOption("meta.mysql.user")
          .getOrElse(throw new IllegalArgumentException("配置项 meta.mysql.user 是必需的"))
        val mysqlPassword = spark.conf.getOption("meta.mysql.password")
          .getOrElse(throw new IllegalArgumentException("配置项 meta.mysql.password 是必需的"))
        
        createMySQLMetaTableManager(spark, mysqlUrl, mysqlUser, mysqlPassword)
    }
  }

  /**
   * 使用默认配置创建MetaTableManager
   *
   * @param spark SparkSession实例
   * @return MetaTableManager实例
   */
  def createDefault(spark: SparkSession): MetaTableManager = {
    createFromSparkConfig(spark)
  }
}

/**
 * 元数据表管理器配置类
 *
 * @param storageType 存储类型
 * @param mysqlUrl MySQL URL（MySQL存储类型时必需）
 * @param mysqlUser MySQL用户名（MySQL存储类型时必需）
 * @param mysqlPassword MySQL密码（MySQL存储类型时必需）
 */
case class MetaTableManagerConfig(
  storageType: MetaTableManagerFactory.StorageType.Value = MetaTableManagerFactory.StorageType.HUDI,
  mysqlUrl: Option[String] = None,
  mysqlUser: Option[String] = None,
  mysqlPassword: Option[String] = None
) {
  
  /**
   * 验证配置的有效性
   *
   * @throws IllegalArgumentException 当配置无效时抛出
   */
  def validate(): Unit = {
    storageType match {
      case MetaTableManagerFactory.StorageType.MYSQL =>
        require(mysqlUrl.isDefined, "MySQL URL是必需的")
        require(mysqlUser.isDefined, "MySQL用户名是必需的")
        require(mysqlPassword.isDefined, "MySQL密码是必需的")
      case _ =>
        // 不需要额外配置
    }
  }
  
  /**
   * 转换为Map格式
   *
   * @return 配置Map
   */
  def toMap: Map[String, String] = {
    Map(
      "storage.type" -> storageType.toString.toLowerCase,
      "mysql.url" -> mysqlUrl.getOrElse(""),
      "mysql.user" -> mysqlUser.getOrElse(""),
      "mysql.password" -> mysqlPassword.getOrElse("")
    ).filter(_._2.nonEmpty)
  }
} 