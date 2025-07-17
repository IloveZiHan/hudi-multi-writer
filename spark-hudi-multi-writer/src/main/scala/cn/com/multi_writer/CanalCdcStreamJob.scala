package cn.com.multi_writer

import cn.com.multi_writer.etl.{CanalBatchProcessor, DataCleaner}
import cn.com.multi_writer.meta.{MetaTableManager, MetaTableManagerFactory, TableConfig}
import cn.com.multi_writer.sink.{HudiConcurrentWriter, HudiConcurrentWriterConfig, WriteTask}
import cn.com.multi_writer.source.CanalKafkaSource
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

/**
 * Canal CDC 流处理作业
 * 用于消费Canal binlog消息并写入Hudi表
 */
object CanalCdcStreamJob {

    private val logger: Logger = LoggerFactory.getLogger(CanalCdcStreamJob.getClass)
    @volatile private var globalConcurrentWriter: Option[HudiConcurrentWriter] = None
    private val writerLock = new Object()

    def main(args: Array[String]): Unit = {
        val spark = SparkSessionManager.createSparkSession("spark-hudi-cdc-canal-stream-job")

        try {
            logger.info("=== Canal CDC Stream Job启动（全局并发写入器模式） ===")
            val configs = SparkSessionManager.getStreamingConfigs(spark)
            val mysqlUrl = configs("mysqlUrl")
            val mysqlUser = configs("mysqlUser")
            val mysqlPassword = configs("mysqlPassword")

            // 创建相关实例
            val metaManager = MetaTableManagerFactory.createMySQLMetaTableManager(spark, mysqlUrl, mysqlUser, mysqlPassword)
            val kafkaSource = new CanalKafkaSource(spark)
            val batchProcessor = new CanalBatchProcessor(spark)
            val dataCleaner = new DataCleaner(spark)

            // 创建HudiConcurrentWriter配置
            val concurrentWriterConfig = HudiConcurrentWriterConfig(
                maxConcurrency = spark.conf.getOption("spark.hudi.concurrent.max").getOrElse("3").toInt,
                timeoutSeconds = spark.conf.getOption("spark.hudi.concurrent.timeout").getOrElse("600").toLong,
                failFast = spark.conf.getOption("spark.hudi.concurrent.failfast").getOrElse("true").toBoolean
            )

            logger.info(s"HudiConcurrentWriter配置 - 最大并发数: ${concurrentWriterConfig.maxConcurrency}, 超时时间: ${concurrentWriterConfig.timeoutSeconds}秒")

            // 创建全局HudiConcurrentWriter实例
            writerLock.synchronized {
                globalConcurrentWriter = Some(HudiConcurrentWriter.withConfig(spark, concurrentWriterConfig))
                logger.info("✓ 已创建全局HudiConcurrentWriter实例")
            }

            // 创建Canal解析数据流
            val parsedStream = kafkaSource.createParsedStream()

            // 启动流处理
            val query = parsedStream.writeStream
                .outputMode("append")
                .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
                    processBatchOptimized(batchDF.sparkSession, batchDF, batchId, batchProcessor, metaManager)
                }
                .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("5 seconds"))
                .start()

            logger.info("Canal CDC流处理已启动，使用全局并发写入器模式...")
            query.awaitTermination()

        } catch {
            case e: Exception =>
                logger.error(s"Canal CDC流处理出错: ${e.getMessage}", e)
        } finally {
            // 关闭全局写入器
            writerLock.synchronized {
                globalConcurrentWriter.foreach {
                    writer =>
                        logger.info("正在关闭全局HudiConcurrentWriter...")
                        writer.shutdown()
                        logger.info("✓ 全局HudiConcurrentWriter已关闭")
                }
                globalConcurrentWriter = None
            }
            spark.stop()
            logger.info("Canal CDC流处理程序执行完成")
        }
    }

    /**
     * 优化后的批次处理函数
     */
    def processBatchOptimized(spark: SparkSession, batchDF: DataFrame, batchId: Long, batchProcessor: CanalBatchProcessor, metaManager: MetaTableManager): Unit = {

        val startTime = System.currentTimeMillis()
        logger.info(s"========== 处理Canal批次 $batchId (全局并发写入器模式) ==========")

        val configs = SparkSessionManager.getStreamingConfigs(spark)

        if (batchDF.isEmpty) {
            logger.info("本批次无Canal数据")
            return
        }

        try {
            // 获取全局并发写入器实例
            val concurrentWriter = writerLock.synchronized {
                globalConcurrentWriter.getOrElse {
                    throw new IllegalStateException("全局HudiConcurrentWriter实例未初始化")
                }
            }

            if (!concurrentWriter.isActive) {
                throw new IllegalStateException("全局HudiConcurrentWriter已关闭")
            }

            // 清理遗留任务
            val clearedTaskCount = concurrentWriter.clearPendingTasks()
            if (clearedTaskCount > 0) {
                logger.warn(s"清理了 $clearedTaskCount 个遗留任务")
            }

            logger.info(s"使用全局并发写入器 - ${concurrentWriter.getPoolStats}")

            // 获取动态表配置
            val applicationName = configs("applicationName")
            val dynamicTableConfigs = getDynamicTableConfigs(metaManager, applicationName)

            if (dynamicTableConfigs.isEmpty) {
                logger.info("未找到已上线的表配置，跳过本批次处理")
                return
            }

            logger.info(s"动态获取到 ${dynamicTableConfigs.length} 个已上线表配置")

            val cachedDF = batchDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
            val cachedCount = cachedDF.count()

            logger.info(s"缓存Canal数据量: $cachedCount 条")

            // 显示部分数据用于调试
            if (logger.isDebugEnabled && cachedCount > 0) {
                logger.debug("Canal数据样例:")
                cachedDF.show(5, truncate = false)
            }

            // 使用并发写入器处理数据
            transformationWithConcurrentWriter(spark, cachedDF, batchId, batchProcessor, concurrentWriter, dynamicTableConfigs)

            logger.info(s"Canal批次 $batchId 所有Hudi表并发写入完成")

            // 释放缓存
            cachedDF.unpersist()
            val endTime = System.currentTimeMillis()
            val processingTime = endTime - startTime

            logger.info(s"Canal批次处理完成，耗时: ${processingTime}ms")

        } catch {
            case e: Exception =>
                logger.error(s"Canal批次 $batchId 处理过程中出错: ${e.getMessage}", e)
                throw new RuntimeException(s"Canal批次 $batchId 处理过程中出错")
        }

        logger.info("========================================")
    }

    /**
     * 获取动态表配置
     */
    def getDynamicTableConfigs(metaManager: MetaTableManager, applicationName: String): Seq[TableConfig] = {
        try {
            val onlineTablesDF = metaManager.queryTablesByApplicationName(applicationName)

            if (onlineTablesDF.isEmpty) {
                logger.info("元数据表中没有已上线的表")
                return Seq.empty
            }

            val onlineTables = onlineTablesDF.collect()
            logger.info(s"从元数据表获取到 ${onlineTables.length} 个已上线表")

            onlineTables.map { row =>
                val id = row.getAs[String]("id")
                val schemaJson = row.getAs[String]("schema")
                val status = row.getAs[Boolean]("status")
                val isPartitioned = row.getAs[Boolean]("is_partitioned")
                val partitionExpr = row.getAs[String]("partition_expr")
                val hoodieConfig = row.getAs[String]("hoodie_config")
                val tags = row.getAs[String]("tags")
                val description = Option(row.getAs[String]("description")).getOrElse("")
                val sourceDb = row.getAs[String]("source_db")
                val sourceTable = row.getAs[String]("source_table")
                val dbType = row.getAs[String]("db_type")

                logger.info(s"处理Canal表配置: $id - $description (源: $sourceDb.$sourceTable)")

                val schema = parseSchemaFromJson(schemaJson)
                val fieldMappings = schema.fields.map(field => (field.name, field.dataType)).toSeq

                logger.info(s"Canal表 $id 包含 ${fieldMappings.length} 个字段")

                TableConfig(id, fieldMappings, status, isPartitioned, Some(tags), Some(description), Some(sourceDb), Some(sourceTable), Some(dbType), Some(partitionExpr), Some(hoodieConfig))
            }.toSeq

        } catch {
            case e: Exception =>
                logger.error(s"获取Canal动态表配置失败: ${e.getMessage}", e)
                Seq.empty
        }
    }

    /**
     * 解析schema JSON字符串
     */
    def parseSchemaFromJson(schemaJson: String): StructType = {
        try {
            DataType.fromJson(schemaJson).asInstanceOf[StructType]
        } catch {
            case e: Exception =>
                logger.error(s"解析Canal schema JSON失败: ${e.getMessage}")
                logger.info(s"Canal Schema JSON: ${schemaJson.take(200)}...")
                throw e
        }
    }

    /**
     * 使用并发写入器处理数据
     */
    private def transformationWithConcurrentWriter(spark: SparkSession,
                                                   cachedDF: DataFrame,
                                                   batchId: Long,
                                                   batchProcessor: CanalBatchProcessor,
                                                   concurrentWriter: HudiConcurrentWriter,
                                                   dynamicTableConfigs: Seq[TableConfig]): Unit = {
        try {
            logger.info("开始执行Canal数据预处理（动态表配置 + 并发写入）...")
            logger.info(s"使用Canal动态表配置数量: ${dynamicTableConfigs.length}")

            // 使用CanalBatchProcessor进行数据预处理
            val transformedDataFrames = batchProcessor.batchTransform(cachedDF, dynamicTableConfigs)

            logger.info(s"Canal数据预处理完成，转换后DataFrame数量: ${transformedDataFrames.length}")

            // 收集写入任务
            val writeTasks = transformedDataFrames.zip(dynamicTableConfigs).zipWithIndex.collect {
                case ((transformedDF, tableConfig), index) if !transformedDF.isEmpty =>
                    val recordCount = transformedDF.count()

                    if (recordCount > 0) {
                        val description = tableConfig.description.map(d => s" - $d").getOrElse("")
                        logger.info(s"准备Canal写入任务[$index] (${tableConfig.id}$description) 记录数: $recordCount")

                        val basePath = SparkSessionManager.getStreamingConfigs(spark)("hudiBasePath")
                        val hudiTableName = tableConfig.id
                        val hudiTablePath = s"$basePath/$hudiTableName"
                        val hudiOptions = tableConfig.getHudiOptions()
                        val taskId = s"canal_batch_${batchId}_table_${tableConfig.id}_${index}"

                        Some(WriteTask(
                            batchDF = transformedDF,
                            hudiTableName = hudiTableName,
                            hudiTablePath = hudiTablePath,
                            taskId = taskId,
                            options = hudiOptions
                        ))
                    } else {
                        logger.info(s"跳过Canal空DataFrame[$index] (${tableConfig.id})")
                        None
                    }
            }.flatten

            if (writeTasks.nonEmpty) {
                logger.info(s"准备并发执行 ${writeTasks.length} 个Canal写入任务...")

                val results = concurrentWriter.submitAndExecute(writeTasks)

                val successCount = results.count(_.isSuccess)
                val failureCount = results.count(_.isFailure)
                val totalRecords = results.filter(_.isSuccess).map(_.executionTime).sum

                logger.info(s"Canal并发写入完成 - 成功: $successCount, 失败: $failureCount, 总耗时: ${totalRecords}ms")

                results.foreach {
                    result =>
                        if (result.isSuccess) {
                            logger.info(s"✓ Canal写入成功: ${result.hudiTableName}, 耗时: ${result.executionTime}ms")
                        } else {
                            logger.error(s"✗ Canal写入失败: ${result.hudiTableName}, 错误: ${result.errorMsg.getOrElse("未知错误")}")
                        }
                }
            } else {
                logger.info("没有需要执行的Canal写入任务")
            }

        } catch {
            case e: Exception =>
                logger.error(s"Canal数据预处理过程中出错: ${e.getMessage}", e)
                throw new RuntimeException(s"Canal数据预处理过程中出错")
        }
    }

    /**
     * 获取全局并发写入器实例
     */
    def getGlobalConcurrentWriter: Option[HudiConcurrentWriter] = {
        writerLock.synchronized {
            globalConcurrentWriter
        }
    }

    /**
     * 检查全局写入器是否可用
     */
    def isGlobalWriterAvailable: Boolean = {
        writerLock.synchronized {
            globalConcurrentWriter.exists(_.isActive)
        }
    }
}