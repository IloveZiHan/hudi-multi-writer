package cn.com.multi_writer.meta

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import java.sql.Timestamp

/**
 * 元数据表管理器的抽象接口
 *
 * 定义了查询Hudi表元数据的方法，专门为Structured Streaming程序服务，包括：
 * - 基于状态的表查询
 */
trait MetaTableManager {

  /**
   * 获取SparkSession实例
   */
  def spark: SparkSession

  /**
   * 获取当前应用程序关联的Hudi表
   *
   * @return DataFrame包含指定状态的表记录
   */
  def queryTablesByApplicationName(applicationName: String): DataFrame
}

/**
 * 表状态常量定义
 */
object TableStatus {
  val OFFLINE = 0 // 离线
  val ONLINE = 1  // 在线
}

/**
 * 元数据表记录的case class定义
 */
case class MetaHudiTableRecord(
                                id: String,
                                schema: String,
                                status: Int,
                                is_partitioned: Boolean = true,
                                partition_expr: Option[String],
                                hoodie_config: Option[String],
                                tags: Option[String],
                                description: Option[String],
                                source_db: Option[String],
                                source_table: Option[String],
                                db_type: Option[String],
                                create_time: Timestamp,
                                update_time: Timestamp,
                                cdc_delete_flag: Option[Int] = Some(0)
                              ) 