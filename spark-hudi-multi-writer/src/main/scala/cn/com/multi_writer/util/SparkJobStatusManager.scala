package cn.com.multi_writer.util

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * Spark Job状态管理器
 *
 * 基于Hudi的HoodieSparkEngineContext.setJobStatus方法封装，
 * 用于将统计信息设置到Spark WebUI，而不是使用show()方法展示。
 *
 * 主要功能：
 * 1. 封装类似于HoodieSparkEngineContext的setJobStatus方法
 * 2. 将DataFrame统计结果转换为格式化字符串
 * 3. 设置到Spark WebUI的Job描述中
 * 4. 支持批次级别的状态管理和更新
 */
object SparkJobStatusManager {

    /**
     * 设置Job状态到Spark WebUI
     * 类似于HoodieSparkEngineContext.setJobStatus方法
     *
     * @param spark SparkSession实例
     * @param activeModule 活动模块名称
     * @param activityDescription 活动描述信息
     */
    def setJobStatus(spark: SparkSession, activeModule: String, activityDescription: String): Unit = {
        spark.sparkContext.setJobGroup(activeModule, activityDescription, interruptOnCancel = true)
    }

    /**
     * 获取DataFrame统计信息并格式化为字符串
     * 不执行show()操作，而是收集数据并格式化
     *
     * @param df 待统计的DataFrame
     * @param maxRows 最大显示行数，默认20
     * @param truncate 是否截断长字符串，默认false
     * @return 格式化后的统计信息字符串
     */
    def collectAndFormatDataFrame(df: DataFrame, maxRows: Int = 20, truncate: Boolean = false): String = {
        try {
            // 收集数据而不是show()
            val rows = df.collect().take(maxRows)
            val schema = df.schema

            if (rows.isEmpty) {
                return "空数据集"
            }

            val columnNames = schema.fieldNames
            val maxColumnLength = if (truncate) 20 else Int.MaxValue

            // 构建表格格式的字符串
            val header = columnNames.mkString(" | ")
            val separator = columnNames.map(_ => "---").mkString(" | ")
            val dataRows = rows.map { row =>
                row.toSeq.zipWithIndex.map { case (value, idx) =>
                    val valueStr = if (value == null) "null" else value.toString
                    if (truncate && valueStr.length > maxColumnLength) {
                        valueStr.take(maxColumnLength) + "..."
                    } else {
                        valueStr
                    }
                }.mkString(" | ")
            }

            (header +: separator +: dataRows).mkString("\n")

        } catch {
            case e: Exception =>
                s"获取统计信息时出错: ${e.getMessage}"
        }
    }

    /**
     * 设置批次处理的完整统计信息到WebUI
     *
     * @param spark SparkSession实例
     * @param batchId 批次ID
     * @param totalCount 总数据量
     * @param eventTypeStats 事件类型统计信息
     * @param tableOperationStats 表操作统计信息
     * @param kafkaMetadataStats Kafka元数据统计信息
     */
    def setBatchProcessingStatus(spark: SparkSession,
                                 batchId: Long,
                                 totalCount: Long,
                                 eventTypeStats: String,
                                 tableOperationStats: String,
                                 kafkaMetadataStats: String): Unit = {

        val statusMessage = s"""
                               |批次ID: $batchId | 数据量: $totalCount
                               |事件类型统计: $eventTypeStats
                               |表操作统计: $tableOperationStats
                               |Kafka元数据: $kafkaMetadataStats
    """.stripMargin.replaceAll("\n", " | ")

        setJobStatus(spark, s"TdSQL_CDC_Batch_$batchId", statusMessage)
    }

    /**
     * 获取事件类型统计信息
     *
     * @param df 数据DataFrame
     * @return 格式化的事件类型统计字符串
     */
    def getEventTypeStats(df: DataFrame): String = {
        try {
            val eventStats = df.groupBy("eventtypestr")
                .count()
                .orderBy(desc("count"))
                .collect()
                .take(5) // 只取前5个事件类型
                .map(row => s"${row.getString(0)}(${row.getLong(1)})")
                .mkString(", ")

            if (eventStats.nonEmpty) eventStats else "无事件统计"
        } catch {
            case e: Exception =>
                s"事件统计错误: ${e.getMessage}"
        }
    }

    /**
     * 获取表操作统计信息
     *
     * @param df 数据DataFrame
     * @return 格式化的表操作统计字符串
     */
    def getTableOperationStats(df: DataFrame): String = {
        try {
            val tableStats = df.filter(col("db").isNotNull && col("table").isNotNull &&
                    col("db") =!= "" && col("table") =!= "")
                .groupBy("db", "table")
                .count()
                .orderBy(desc("count"))
                .collect()
                .take(3) // 只取前3个表操作
                .map(row => s"${row.getString(0)}.${row.getString(1)}(${row.getLong(2)})")
                .mkString(", ")

            if (tableStats.nonEmpty) tableStats else "无表操作"
        } catch {
            case e: Exception =>
                s"表操作统计错误: ${e.getMessage}"
        }
    }

    /**
     * 获取Kafka元数据统计信息
     *
     * @param df 数据DataFrame
     * @return 格式化的Kafka元数据统计字符串
     */
    def getKafkaMetadataStats(df: DataFrame): String = {
        try {
            val kafkaStats = df.groupBy("kafka_topic", "kafka_partition")
                .agg(
                    count("*").as("record_count"),
                    min("kafka_offset").as("min_offset"),
                    max("kafka_offset").as("max_offset")
                )
                .collect()
                .map { row =>
                    val topic = row.getString(0)
                    val partition = row.getInt(1)
                    val recordCount = row.getLong(2)
                    val minOffset = row.getLong(3)
                    val maxOffset = row.getLong(4)
                    s"分区$partition: 记录数$recordCount, 偏移量$minOffset-$maxOffset"
                }
                .mkString(", ")

            if (kafkaStats.nonEmpty) kafkaStats else "无Kafka元数据"
        } catch {
            case e: Exception =>
                s"Kafka元数据统计错误: ${e.getMessage}"
        }
    }

    /**
     * 设置数据处理阶段的状态信息
     *
     * @param spark SparkSession实例
     * @param batchId 批次ID
     * @param stage 处理阶段 (如: "数据清洗", "数据分析", "统计汇总"等)
     * @param description 详细描述
     */
    def setProcessingStageStatus(spark: SparkSession, batchId: Long, stage: String, description: String): Unit = {
        setJobStatus(spark, s"Batch_${batchId}_$stage", description)
    }

    /**
     * 设置数据量统计状态
     *
     * @param spark SparkSession实例
     * @param batchId 批次ID
     * @param originalCount 原始数据量
     * @param cleanedCount 清洗后数据量
     * @param partitionCount 分区数
     */
    def setDataVolumeStatus(spark: SparkSession, batchId: Long, originalCount: Long, cleanedCount: Long, partitionCount: Int): Unit = {
        val description = s"批次$batchId: 原始${originalCount}条 -> 清洗后${cleanedCount}条, 分区数: $partitionCount"
        setJobStatus(spark, s"Batch_${batchId}_DataVolume", description)
    }
} 