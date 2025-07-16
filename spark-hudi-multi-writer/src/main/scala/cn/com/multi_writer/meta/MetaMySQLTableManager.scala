package cn.com.multi_writer.meta

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.{Logger, LoggerFactory}

import java.sql.{Connection, DriverManager}
import java.util.Properties

/**
 * MySQL版本的元数据表管理器
 *
 * 专门为Structured Streaming程序服务，提供查询MySQL存储的Hudi表元数据功能
 * 基于hoodie_meta_table表结构实现
 *
 * @param spark         SparkSession实例
 * @param mysqlUrl      MySQL数据库连接URL
 * @param mysqlUser     MySQL数据库用户名
 * @param mysqlPassword MySQL数据库密码
 */
class MetaMySQLTableManager(val spark: SparkSession,
                            mysqlUrl: String,
                            mysqlUser: String,
                            mysqlPassword: String) extends MetaTableManager {

    private val logger: Logger = LoggerFactory.getLogger(classOf[MetaMySQLTableManager])

    override def queryTablesByApplicationName(applicationName: String): DataFrame = {
        try {
            // 连接两个表，查询指定应用下的所有上线Hudi表
            val sql = s"""
                SELECT 
                    hmt.id,
                    hmt.schema,
                    hmt.status,
                    hmt.is_partitioned,
                    hmt.partition_expr,
                    hmt.hoodie_config,
                    hmt.tags,
                    hmt.description,
                    hmt.source_db,
                    hmt.source_table,
                    hmt.db_type,
                    hmt.create_time,
                    hmt.update_time,
                    hmt.cdc_delete_flag,
                    hmta.application_name,
                    hmta.table_type
                FROM hoodie_meta_table hmt
                JOIN hoodie_meta_table_application hmta ON hmt.id = hmta.table_id
                WHERE hmta.application_name = '$applicationName'
                  AND hmt.status = 1
                  AND hmt.cdc_delete_flag = 0
                  AND hmta.table_type = 'hudi'
            """

            logger.info(s"查询表元数据SQL: $sql")
            
            spark.read
                .format("jdbc")
                .option("url", mysqlUrl)
                .option("query", sql)
                .option("user", mysqlUser)
                .option("password", mysqlPassword)
                .load()
        } catch {
            case e: Exception =>
                logger.error(s"查询表元数据失败: ${e.getMessage}", e)
                spark.emptyDataFrame
        }
    }
} 