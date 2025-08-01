package cn.com.multi_writer.meta

import org.apache.spark.sql.types.DataType
import java.sql.Timestamp
import com.alibaba.fastjson.JSON
import scala.collection.JavaConverters._
import org.slf4j.{Logger, LoggerFactory}

/**
 * 表配置样例类
 * 
 * 与 MetaHudiTableManager 中的 meta_hudi_table DDL 保持一致
 * 包含了元数据表的所有字段定义
 */
case class TableConfig(id: String, fieldMapping: Seq[(String, DataType)], status: Boolean, isPartitioned: Boolean, tags: Option[String] = None, description: Option[String] = None, sourceDb: Option[String] = None, sourceTable: Option[String] = None, dbType: Option[String] = None, partitionExpr: Option[String] = None, hoodieConfig: Option[String] = None) {
    
    // 创建logger实例
    private val logger: Logger = LoggerFactory.getLogger(classOf[TableConfig])
    
    /**
     * 获取表标识符
     */
    def tableId: String = id

    /**
     * 获取分区类型描述
     */
    def partitionTypeDescription: String = if (isPartitioned) "分区表" else "非分区表"

    /**
     * 获取标签列表
     */
    def tagList: List[String] = {
        tags.map(_.split(",").map(_.trim).toList).getOrElse(List.empty)
    }

    /**
     * 获取完整的源表标识（数据库名.表名）
     */
    def fullSourceTableName: Option[String] = {
        for {
            db <- sourceDb
            table <- sourceTable
        } yield s"$db.$table"
    }

    /**
     * 获取Hudi配置选项
     * 
     * 解析hoodieConfig字段中的JSON字符串并转换为Map[String, String]
     * 同时添加基础的Hudi配置选项
     * 
     * @return Map[String, String] 包含基础配置和自定义配置的映射
     */
    def getHudiOptions(): Map[String, String] = {
        // json解析hoodieConfig
        val hoodieConfigMap = JSON.parseObject(hoodieConfig.getOrElse("{}"))
        // 将hoodieConfigMap转换为Map[String, String]
        hoodieConfigMap.asScala.toMap.mapValues(_.toString)
    }

    /**
     * 打印表的基本信息
     */
    def printInfo(): Unit = {
        logger.info(s"表ID: $id")
        logger.info(s"状态: $status")
        logger.info(s"分区类型: $partitionTypeDescription")
        logger.info(s"源表: ${fullSourceTableName.getOrElse("N/A")}")
        logger.info(s"数据库类型: ${dbType.getOrElse("N/A")}")
        logger.info(s"分区表达式: ${partitionExpr.getOrElse("N/A")}")
        logger.info(s"Hudi配置: ${hoodieConfig.getOrElse("N/A")}")
        logger.info(s"标签: ${tags.getOrElse("无")}")
        logger.info(s"描述: ${description.getOrElse("无")}")
    }
}