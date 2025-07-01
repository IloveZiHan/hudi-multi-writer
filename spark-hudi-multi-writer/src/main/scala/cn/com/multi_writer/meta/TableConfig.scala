package cn.com.multi_writer.meta

import org.apache.spark.sql.types.DataType
import java.sql.Timestamp

/**
 * 表配置样例类
 * 
 * 与 MetaHudiTableManager 中的 meta_hudi_table DDL 保持一致
 * 包含了元数据表的所有字段定义
 */
case class TableConfig(
                          id: String,
                          fieldMapping: Seq[(String, DataType)],
                          status: Int,
                          isPartitioned: Boolean,
                          tags: Option[String] = None,
                          description: Option[String] = None,
                          sourceDb: Option[String] = None,
                          sourceTable: Option[String] = None,
                          dbType: Option[String] = None,
                          partitionExpr: Option[String] = None,
                          hoodieConfig: Option[String] = None
                      ) {
    
    /**
     * 获取表标识符
     */
    def tableId: String = id

    /**
     * 获取表状态描述
     */
    def statusDescription: String = status match {
        case 0 => "未上线"
        case 1 => "已上线"
        case _ => "未知状态"
    }

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
     * 打印表的基本信息
     */
    def printInfo(): Unit = {
        println(s"表ID: $id")
        println(s"状态: $statusDescription")
        println(s"分区类型: $partitionTypeDescription")
        println(s"源表: ${fullSourceTableName.getOrElse("N/A")}")
        println(s"数据库类型: ${dbType.getOrElse("N/A")}")
        println(s"分区表达式: ${partitionExpr.getOrElse("N/A")}")
        println(s"Hudi配置: ${hoodieConfig.getOrElse("N/A")}")
        println(s"标签: ${tags.getOrElse("无")}")
        println(s"描述: ${description.getOrElse("无")}")
    }
}