package cn.com.multi_writer

import cn.com.multi_writer.etl.{DataCleaner, BuriedpointBatchProcessor}
import cn.com.multi_writer.meta.{MetaTableManager, MetaTableManagerFactory, TableConfig}
import cn.com.multi_writer.sink.{HudiConcurrentWriter, HudiConcurrentWriterConfig, HudiWriter, WriteTask}
import cn.com.multi_writer.source.BuriedpointKafkaSource
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}


object BuriedpointCdcStreamJob {

    private val logger: Logger = LoggerFactory.getLogger(BuriedpointCdcStreamJob.getClass)
    
    // 【关键优化】全局HudiConcurrentWriter实例，生命周期与流作业绑定
    @volatile private var globalConcurrentWriter: Option[HudiConcurrentWriter] = None
    
    // 用于同步访问全局写入器的锁
    private val writerLock = new Object()

    def main(args: Array[String]): Unit = {
        val spark = SparkSessionManager.createSparkSession("spark-hudi-cdc-buriedpoint-stream-job")

        try {
            logger.info("=== 优化后的埋点数据 CDC Stream Job启动（全局并发写入器模式） ===")
            // 获取配置
            val configs = SparkSessionManager.getStreamingConfigs(spark)
            // 获取mysql url, user, password
            val mysqlUrl = configs("mysqlUrl")
            val mysqlUser = configs("mysqlUser")
            val mysqlPassword = configs("mysqlPassword")

            // 创建相关实例
            val metaManager = MetaTableManagerFactory.createMySQLMetaTableManager(spark, mysqlUrl, mysqlUser, mysqlPassword)
            val kafkaSource = new BuriedpointKafkaSource(spark)
            val batchProcessor = new BuriedpointBatchProcessor(spark)
            val dataCleaner = new DataCleaner(spark)
            
            // 创建HudiConcurrentWriter配置
            val concurrentWriterConfig = HudiConcurrentWriterConfig(
                maxConcurrency = spark.conf.getOption("spark.hudi.concurrent.max").getOrElse("3").toInt,
                timeoutSeconds = spark.conf.getOption("spark.hudi.concurrent.timeout").getOrElse("600").toLong,
                failFast = spark.conf.getOption("spark.hudi.concurrent.failfast").getOrElse("true").toBoolean
            )
            
            logger.info(s"HudiConcurrentWriter配置 - 最大并发数: ${concurrentWriterConfig.maxConcurrency}, 超时时间: ${concurrentWriterConfig.timeoutSeconds}秒, 快速失败: ${concurrentWriterConfig.failFast}")

            // 【关键优化】在流作业开始时创建全局HudiConcurrentWriter实例
            writerLock.synchronized {
                globalConcurrentWriter = Some(HudiConcurrentWriter.withConfig(spark, concurrentWriterConfig))
                logger.info("✓ 已创建全局HudiConcurrentWriter实例，生命周期与流作业绑定")
            }

            // 创建数据流
            val parsedStream = kafkaSource.createParsedStream()

            // 启动流处理 - 使用优化后的批处理函数
            val query = parsedStream.writeStream
                .outputMode("append")
                .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
                    processBatchOptimized(batchDF.sparkSession, batchDF, batchId, batchProcessor, dataCleaner, metaManager)
                }
                .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("5 seconds"))
                .start()

            logger.info("埋点数据流处理已启动，使用全局并发写入器模式...")
            query.awaitTermination()

        } catch {
            case e: Exception =>
                logger.error(s"埋点数据处理程序执行出错: ${e.getMessage}", e)
        } finally {
            // 【关键】在流作业结束时释放全局写入器资源
            writerLock.synchronized {
                globalConcurrentWriter.foreach { writer =>
                    logger.info("正在关闭全局HudiConcurrentWriter...")
                    writer.shutdown()
                    logger.info("✓ 全局HudiConcurrentWriter已关闭")
                }
                globalConcurrentWriter = None
            }
            spark.stop()
            logger.info("埋点数据处理程序执行完成")
        }
    }

    /**
     * 优化后的批次处理函数 - 使用全局HudiConcurrentWriter实例
     * 
     * 性能优化：
     * 1. 使用全局HudiConcurrentWriter，避免频繁创建/销毁线程池
     * 2. 减少对象创建开销，提升内存使用效率
     * 3. 减少GC压力，提升整体性能
     */
    def processBatchOptimized(spark: SparkSession,
                            batchDF: DataFrame,
                            batchId: Long,
                            batchProcessor: BuriedpointBatchProcessor,
                            dataCleaner: DataCleaner,
                            metaManager: MetaTableManager): Unit = {
        
        val startTime = System.currentTimeMillis()
        logger.info(s"========== 处理埋点数据批次 $batchId (全局并发写入器模式) ==========")

        val configs = SparkSessionManager.getStreamingConfigs(spark)

        if (batchDF.isEmpty) {
            logger.info("本批次无埋点数据")
            return
        }
        
        try {
            // 获取全局并发写入器实例
            val concurrentWriter = writerLock.synchronized {
                globalConcurrentWriter.getOrElse {
                    throw new IllegalStateException("全局HudiConcurrentWriter实例未初始化，请检查流作业启动流程")
                }
            }

            // 验证写入器状态
            if (!concurrentWriter.isActive) {
                throw new IllegalStateException("全局HudiConcurrentWriter已关闭，无法处理批次")
            }

            // 清理上次batch可能遗留的任务
            val clearedTaskCount = concurrentWriter.clearPendingTasks()
            if (clearedTaskCount > 0) {
                logger.warn(s"清理了 $clearedTaskCount 个遗留任务")
            }

            // 记录写入器状态
            logger.info(s"使用全局并发写入器 - ${concurrentWriter.getPoolStats}")

            // 获取动态表配置
            val applicationName = configs("applicationName")
            val dynamicTableConfigs = getDynamicTableConfigs(metaManager, applicationName)
            
            if (dynamicTableConfigs.isEmpty) {
                logger.info("未找到已上线的埋点数据表配置，跳过本批次处理")
                return
            }

            logger.info(s"动态获取到 ${dynamicTableConfigs.length} 个已上线埋点数据表配置")

            val cachedDF = batchDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
            val cachedCount = cachedDF.count()

            logger.info(s"缓存埋点数据量: $cachedCount 条")

            // 【关键优化】直接使用全局并发写入器，无需创建和销毁
            transformationWithConcurrentWriter(spark, cachedDF, batchId, 
                                             batchProcessor, concurrentWriter, dynamicTableConfigs)

            logger.info(s"埋点数据批次 $batchId 所有Hudi表并发写入完成")

            // 释放缓存
            cachedDF.unpersist()

            val endTime = System.currentTimeMillis()
            val processingTime = endTime - startTime
            
            logger.info(s"埋点数据批次处理完成，耗时: ${processingTime}ms")

        } catch {
            case e: Exception =>
                logger.error(s"埋点数据批次 $batchId 处理过程中出错: ${e.getMessage}", e)
                throw new RuntimeException(s"埋点数据批次 $batchId 处理过程中出错: ${e.getMessage}")
        }

        logger.info("========================================")
    }

    /**
     * 获取全局HudiConcurrentWriter实例（用于外部访问）
     */
    def getGlobalConcurrentWriter: Option[HudiConcurrentWriter] = {
        writerLock.synchronized {
            globalConcurrentWriter
        }
    }

    /**
     * 检查全局HudiConcurrentWriter是否可用
     */
    def isGlobalWriterAvailable: Boolean = {
        writerLock.synchronized {
            globalConcurrentWriter.exists(_.isActive)
        }
    }

    /**
     * 从元数据表动态获取埋点数据表配置信息
     */
    def getDynamicTableConfigs(metaManager: MetaTableManager, applicationName: String): Seq[TableConfig] = {
        try {
            // 查询状态为1（已上线）的埋点数据表
            val onlineTablesDF = metaManager.queryTablesByApplicationName(applicationName)
            
            if (onlineTablesDF.isEmpty) {
                logger.info("元数据表中没有已上线的埋点数据表")
                return Seq.empty
            }
            
            val onlineTables = onlineTablesDF.collect()
            logger.info(s"从元数据表获取到 ${onlineTables.length} 个已上线埋点数据表")
            
            // 转换为表配置格式
            val tableConfigs = onlineTables.map { row =>
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
                
                logger.info(s"处理埋点数据表配置: $id - $description")
                
                // 解析schema JSON为StructType
                val schema = parseSchemaFromJson(schemaJson)
                
                // 将StructType转换为字段类型映射
                val fieldMappings = schema.fields.map(field => (field.name, field.dataType)).toSeq
                
                logger.info(s"埋点数据表 $id 包含 ${fieldMappings.length} 个字段")
                
                TableConfig(id, fieldMappings, status, isPartitioned, Some(tags), Some(description), Some(sourceDb), Some(sourceTable), Some(dbType), Some(partitionExpr), Some(hoodieConfig))   
            }.toSeq
            
            tableConfigs
            
        } catch {
            case e: Exception =>
                logger.error(s"获取动态埋点数据表配置失败: ${e.getMessage}", e)
                Seq.empty
        }
    }

    /**
     * 解析schema JSON字符串为StructType
     */
    def parseSchemaFromJson(schemaJson: String): StructType = {
        try {
            // 使用Spark内置的DataType.fromJson方法解析
            DataType.fromJson(schemaJson).asInstanceOf[StructType]
        } catch {
            case e: Exception =>
                logger.error(s"解析埋点数据schema JSON失败: ${e.getMessage}")
                logger.info(s"Schema JSON: ${schemaJson.take(200)}...")
                throw e
        }
    }

    /**
     * 使用HudiConcurrentWriter的埋点数据预处理执行函数
     */
    private def transformationWithConcurrentWriter(spark: SparkSession,
                                                   cachedDF: DataFrame,
                                                   batchId: Long,
                                                   batchProcessor: BuriedpointBatchProcessor,
                                                   concurrentWriter: HudiConcurrentWriter,
                                                   dynamicTableConfigs: Seq[TableConfig]): Unit = {
        try {
            logger.info("开始执行埋点数据预处理（动态表配置 + 并发写入）...")
            logger.info(s"使用动态表配置数量: ${dynamicTableConfigs.length}")
            
            // 使用batchTransform方法进行埋点数据预处理
            val transformedDataFrames = batchProcessor.batchTransform(cachedDF, dynamicTableConfigs)
            
            logger.info(s"埋点数据预处理完成，转换后DataFrame数量: ${transformedDataFrames.length}")
            
            // 收集所有写入任务
            val writeTasks = transformedDataFrames.zip(dynamicTableConfigs).zipWithIndex.collect {
                case ((transformedDF, tableConfig), index) if !transformedDF.isEmpty =>
                    val recordCount = transformedDF.count()
                    
                    if (recordCount > 0) {
                        val description = tableConfig.description.map(d => s" - $d").getOrElse("")
                        logger.info(s"准备写入埋点数据任务[$index] (${tableConfig.id}$description) 记录数: $recordCount")
                        
                        // 构建Hudi表名和路径
                        val basePath = SparkSessionManager.getStreamingConfigs(spark)("hudiBasePath")
                        val hudiTableName = tableConfig.id
                        val hudiTablePath = s"$basePath/$hudiTableName"
                        
                        // 构建Hudi配置选项
                        val hudiOptions = tableConfig.getHudiOptions()
                        
                        // 创建写入任务
                        val taskId = s"buriedpoint_batch_${batchId}_table_${tableConfig.id}_${index}"
                        
                        Some(WriteTask(
                            batchDF = transformedDF,
                            hudiTableName = hudiTableName,
                            hudiTablePath = hudiTablePath,
                            taskId = taskId,
                            options = hudiOptions
                        ))
                    } else {
                        logger.info(s"跳过空DataFrame[$index] (${tableConfig.id})")
                        None
                    }
            }.flatten
            
            if (writeTasks.nonEmpty) {
                logger.info(s"准备并发执行 ${writeTasks.length} 个埋点数据写入任务...")
                
                // 使用并发写入器执行所有任务
                val results = concurrentWriter.submitAndExecute(writeTasks)
                
                // 分析结果
                val successCount = results.count(_.isSuccess)
                val failureCount = results.count(_.isFailure)
                val totalRecords = results.filter(_.isSuccess).map(_.executionTime).sum
                
                logger.info(s"埋点数据并发写入完成 - 成功: $successCount, 失败: $failureCount")
                
                // 详细输出结果
                results.foreach { result =>
                    if (result.isSuccess) {
                        logger.info(s"✓ 埋点数据写入成功: ${result.hudiTableName}, 耗时: ${result.executionTime}ms")
                    } else {
                        logger.error(s"✗ 埋点数据写入失败: ${result.hudiTableName}, 错误: ${result.errorMsg.getOrElse("未知错误")}")
                    }
                }
                
                // 记录总体统计
                val totalTime = results.map(_.executionTime).sum
                val avgTime = if (results.nonEmpty) totalTime / results.length else 0L
                logger.info(s"埋点数据总写入时间: ${totalTime}ms, 平均耗时: ${avgTime}ms")
                
            } else {
                logger.info("没有需要写入的埋点数据")
            }
            
        } catch {
            case e: Exception =>
                logger.error(s"埋点数据预处理过程中出错: ${e.getMessage}", e)
                throw new RuntimeException(s"埋点数据预处理过程中出错: ${e.getMessage}")
        }
    }
}
