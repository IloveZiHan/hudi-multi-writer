package cn.com.multi_writer

import cn.com.multi_writer.etl.{DataCleaner, TdsqlBatchProcessor}
import cn.com.multi_writer.sink.{HudiWriter, HudiConcurrentWriter, HudiConcurrentWriterConfig, WriteTask}
import cn.com.multi_writer.source.KafkaSource
import cn.com.multi_writer.util.SparkJobStatusManager
import cn.com.multi_writer.meta.{MetaHudiTableManager, TableConfig}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}

import java.sql.Timestamp

/**
 * 优化后的TdSQL CDC 流处理作业示例
 *
 * 主要优化点：
 * 1. 使用MetaHudiTableManager.queryTablesByStatus动态获取表配置
 * 2. 在每次processBatch中都获取最新的表信息，支持表配置的动态更新
 * 3. 移除静态的tableConfigs，改为动态获取
 * 4. 支持通过元数据表管理表的上线/下线状态
 * 5. 使用TableConfig样例类替换复杂的元组类型，提高代码可读性
 * 6. 【新增】使用HudiConcurrentWriter实现并发写入，显著提升性能
 * 7. 【新增】支持可配置的并发写入参数，灵活控制并发数和超时时间
 * 8. 【新增】提供详细的写入结果统计和错误处理
 *
 * 配置参数说明：
 * - spark.meta.table.path: 元数据表路径 (默认: /tmp/hudi_tables/meta_hudi_table)
 * - spark.hudi.base.path: Hudi表基础路径 (默认: /tmp/hudi_tables)
 * - spark.debug.show.data: 是否显示调试数据 (默认: false)
 * - spark.hudi.concurrent.max: 最大并发写入数 (默认: 3)
 * - spark.hudi.concurrent.timeout: 并发写入超时时间（秒）(默认: 600)
 * - spark.hudi.concurrent.failfast: 是否快速失败 (默认: true)
 */
object TdsqlCdcStreamJob {

    private val logger: Logger = LoggerFactory.getLogger(TdsqlCdcStreamJob.getClass)

    def main(args: Array[String]): Unit = {
        val spark = SparkSessionManager.createSparkSession("TdSQL CDC Stream Job")

        try {
            logger.info("=== 优化后的TdSQL CDC Stream Job启动 ===")

            // 创建MetaHudiTableManager实例
            val metaManager = new MetaHudiTableManager(spark)

            // 创建相关实例
            val kafkaSource = new KafkaSource(spark)
            val hudiWriter = new HudiWriter(spark)
            val batchProcessor = new TdsqlBatchProcessor(spark)
            val dataCleaner = new DataCleaner(spark)
            
            // 创建HudiConcurrentWriter配置
            val concurrentWriterConfig = HudiConcurrentWriterConfig(
                maxConcurrency = spark.conf.getOption("spark.hudi.concurrent.max").getOrElse("3").toInt,
                timeoutSeconds = spark.conf.getOption("spark.hudi.concurrent.timeout").getOrElse("600").toLong,
                failFast = spark.conf.getOption("spark.hudi.concurrent.failfast").getOrElse("true").toBoolean
            )
            
            logger.info(s"HudiConcurrentWriter配置 - 最大并发数: ${concurrentWriterConfig.maxConcurrency}, 超时时间: ${concurrentWriterConfig.timeoutSeconds}秒, 快速失败: ${concurrentWriterConfig.failFast}")

            // 创建数据流
            val parsedStream = kafkaSource.createParsedStream()

            // 启动流处理
            val query = parsedStream.writeStream
                .outputMode("append")
                .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
                    processBatch(batchDF.sparkSession
                        , batchDF
                        , batchId
                        , hudiWriter
                        , batchProcessor
                        , dataCleaner
                        , metaManager
                        , concurrentWriterConfig)
                }
                .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("5 seconds"))
                .start()

            logger.info("流处理已启动，使用动态表配置...")
            query.awaitTermination()

        } catch {
            case e: Exception =>
                logger.error(s"程序执行出错: ${e.getMessage}", e)
        } finally {
            spark.stop()
            logger.info("程序执行完成")
        }
    }

    /**
     * 使用动态表配置的微批次数据处理函数
     */
    def processBatch(spark: SparkSession
                     , batchDF: DataFrame
                     , batchId: Long
                     , hudiWriter: HudiWriter
                     , batchProcessor: TdsqlBatchProcessor
                     , dataCleaner: DataCleaner
                     , metaManager: MetaHudiTableManager
                     , concurrentWriterConfig: HudiConcurrentWriterConfig): Unit = {
        
        val startTime = System.currentTimeMillis()
        logger.info(s"========== 处理批次 $batchId (动态表配置) ==========")

        val configs = SparkSessionManager.getStreamingConfigs(spark)
        val metaTablePath = configs("metaTablePath")

        if (batchDF.isEmpty) {
            logger.info("本批次无数据")
            return
        }
        
        try {
            // 第一步：从元数据表获取最新的已上线表配置
            val dynamicTableConfigs = getDynamicTableConfigs(metaManager, metaTablePath)
            
            if (dynamicTableConfigs.isEmpty) {
                logger.info("未找到已上线的表配置，跳过本批次处理")
                return
            }

            logger.info(s"动态获取到 ${dynamicTableConfigs.length} 个已上线表配置")

            // 数据清洗
            val cleanedDF = dataCleaner.cleanData(batchDF)
            val cachedDF = cleanedDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
            val cachedCount = cachedDF.count()

            logger.info(s"缓存数据量: $cachedCount 条")

            // 创建HudiConcurrentWriter实例
            val concurrentWriter = HudiConcurrentWriter.withConfig(spark, concurrentWriterConfig)
            
            try {
                // 执行数据预处理：使用动态获取的表配置和并发写入器
                transformationWithConcurrentWriter(spark,
                    cachedDF, 
                    batchId, 
                    batchProcessor, 
                    concurrentWriter, 
                    dynamicTableConfigs)

                logger.info(s"批次 $batchId 所有Hudi表并发写入完成")

            } finally {
                // 确保并发写入器资源释放
                concurrentWriter.shutdown()
            }

            // 释放缓存
            cachedDF.unpersist()

            val endTime = System.currentTimeMillis()
            val processingTime = endTime - startTime
            
            logger.info(s"批次处理完成，耗时: ${processingTime}ms")

        } catch {
            case e: Exception =>
                logger.error(s"批次 $batchId 处理过程中出错: ${e.getMessage}", e)
                throw new RuntimeException(s"批次 $batchId 处理过程中出错: ${e.getMessage}")
        }

        logger.info("========================================")
    }

    /**
     * 从元数据表动态获取表配置信息
     */
    def getDynamicTableConfigs(metaManager: MetaHudiTableManager, metaTablePath: String): Seq[TableConfig] = {
        try {
            logger.info(s"正在从元数据表获取已上线表配置: $metaTablePath")
            
            // 查询状态为1（已上线）的表
            val onlineTablesDF = metaManager.queryTablesByStatus(1, metaTablePath)
            
            if (onlineTablesDF.isEmpty) {
                logger.info("元数据表中没有已上线的表")
                return Seq.empty
            }
            
            val onlineTables = onlineTablesDF.collect()
            logger.info(s"从元数据表获取到 ${onlineTables.length} 个已上线表")
            
            // 转换为表配置格式
            val tableConfigs = onlineTables.map { row =>
                val id = row.getAs[String]("id")
                val schemaJson = row.getAs[String]("schema")
                val status = row.getAs[Int]("status")
                val isPartitioned = row.getAs[Boolean]("is_partitioned")
                val partitionExpr = row.getAs[String]("partition_expr")
                val hoodieConfig = row.getAs[String]("hoodie_config")
                val tags = row.getAs[String]("tags")
                val description = Option(row.getAs[String]("description")).getOrElse("")
                val sourceDb = row.getAs[String]("source_db")
                val sourceTable = row.getAs[String]("source_table")
                val dbType = row.getAs[String]("db_type")
                
                logger.info(s"处理表配置: $id - $description")
                
                // 解析schema JSON为StructType
                val schema = parseSchemaFromJson(schemaJson)
                
                // 将StructType转换为字段类型映射
                val fieldMappings = schema.fields.map(field => (field.name, field.dataType)).toSeq
                
                logger.info(s"表 $id 包含 ${fieldMappings.length} 个字段")
                
                TableConfig(id, fieldMappings, status, isPartitioned, Some(tags), Some(description), Some(sourceDb), Some(sourceTable), Some(dbType), Some(partitionExpr), Some(hoodieConfig))   
            }.toSeq
            
            tableConfigs
            
        } catch {
            case e: Exception =>
                logger.error(s"获取动态表配置失败: ${e.getMessage}", e)
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
                logger.error(s"解析schema JSON失败: ${e.getMessage}")
                logger.info(s"Schema JSON: ${schemaJson.take(200)}...")
                throw e
        }
    }

    /**
     * 使用动态表配置的数据预处理执行函数
     * @deprecated 建议使用 transformationWithConcurrentWriter 获得更好的并发性能
     */
    private def transformation(spark: SparkSession,
                                                   cachedDF: DataFrame,
                                                   batchId: Long,
                                                   batchProcessor: TdsqlBatchProcessor,
                                                   hudiWriter: HudiWriter,
                                                   dynamicTableConfigs: Seq[TableConfig]): Unit = {
        try {
            logger.info("开始执行数据预处理（动态表配置）...")
            logger.info(s"使用动态表配置数量: ${dynamicTableConfigs.length}")
            
            // 使用batchTransform方法进行数据预处理
            val transformedDataFrames = batchProcessor.batchTransform(cachedDF, dynamicTableConfigs)
            
            logger.info(s"数据预处理完成，转换后DataFrame数量: ${transformedDataFrames.length}")
            
            // 批量处理转换后的DataFrame
            val processResults = transformedDataFrames.zip(dynamicTableConfigs).zipWithIndex.map { 
                case ((transformedDF, tableConfig), index) =>
                    processTransformedDataFrameWithDynamic(spark, transformedDF, batchId, tableConfig,
                                                          index, hudiWriter)
            }
            
            val totalWrittenRecords = processResults.sum
            logger.info(s"批量处理完成，总写入记录数: $totalWrittenRecords")
            
        } catch {
            case e: Exception =>
                logger.error(s"数据预处理过程中出错: ${e.getMessage}", e)
                throw new RuntimeException(s"数据预处理过程中出错: ${e.getMessage}")
        }
    }

    /**
     * 使用HudiConcurrentWriter的数据预处理执行函数
     */
    private def transformationWithConcurrentWriter(spark: SparkSession,
                                                   cachedDF: DataFrame,
                                                   batchId: Long,
                                                   batchProcessor: TdsqlBatchProcessor,
                                                   concurrentWriter: HudiConcurrentWriter,
                                                   dynamicTableConfigs: Seq[TableConfig]): Unit = {
        try {
            logger.info("开始执行数据预处理（动态表配置 + 并发写入）...")
            logger.info(s"使用动态表配置数量: ${dynamicTableConfigs.length}")
            
            // 使用batchTransform方法进行数据预处理
            val transformedDataFrames = batchProcessor.batchTransform(cachedDF, dynamicTableConfigs)
            
            logger.info(s"数据预处理完成，转换后DataFrame数量: ${transformedDataFrames.length}")
            
            // 收集所有写入任务
            val writeTasks = transformedDataFrames.zip(dynamicTableConfigs).zipWithIndex.collect {
                case ((transformedDF, tableConfig), index) if !transformedDF.isEmpty =>
                    val recordCount = transformedDF.count()
                    
                    if (recordCount > 0) {
                        val description = tableConfig.description.map(d => s" - $d").getOrElse("")
                        logger.info(s"准备写入任务[$index] (${tableConfig.id}$description) 记录数: $recordCount")
                        
                        // 构建Hudi表名和路径
                        val basePath = SparkSessionManager.getStreamingConfigs(spark)("hudiBasePath")
                        val hudiTableName = tableConfig.id
                        val hudiTablePath = s"$basePath/$hudiTableName"
                        
                        // 构建Hudi配置选项
                        val hudiOptions = tableConfig.getHudiOptions()
                        
                        // 创建写入任务
                        val taskId = s"batch_${batchId}_table_${tableConfig.id}_${index}"
                        
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
                logger.info(s"准备并发执行 ${writeTasks.length} 个写入任务...")
                
                // 使用并发写入器执行所有任务
                val results = concurrentWriter.submitAndExecute(writeTasks)
                
                // 分析结果
                val successCount = results.count(_.isSuccess)
                val failureCount = results.count(_.isFailure)
                val totalRecords = results.filter(_.isSuccess).map(_.executionTime).sum
                
                logger.info(s"并发写入完成 - 成功: $successCount, 失败: $failureCount")
                
                // 详细输出结果
                results.foreach { result =>
                    if (result.isSuccess) {
                        logger.info(s"✓ 写入成功: ${result.hudiTableName}, 耗时: ${result.executionTime}ms")
                    } else {
                        logger.error(s"✗ 写入失败: ${result.hudiTableName}, 错误: ${result.errorMsg.getOrElse("未知错误")}")
                    }
                }
                
                // 记录总体统计
                val totalTime = results.map(_.executionTime).sum
                val avgTime = if (results.nonEmpty) totalTime / results.length else 0L
                logger.info(s"总写入时间: ${totalTime}ms, 平均耗时: ${avgTime}ms")
                
            } else {
                logger.info("没有需要写入的数据")
            }
            
        } catch {
            case e: Exception =>
                logger.error(s"数据预处理过程中出错: ${e.getMessage}", e)
                throw new RuntimeException(s"数据预处理过程中出错: ${e.getMessage}")
        }
    }

    /**
     * 使用动态配置处理单个转换后的DataFrame
     */
    private def processTransformedDataFrameWithDynamic(spark: SparkSession,
                                                      transformedDF: DataFrame,
                                                      batchId: Long,
                                                      tableConfig: TableConfig,
                                                      index: Int,
                                                      hudiWriter: HudiWriter): Long = {
        
        if (transformedDF.isEmpty) {
            logger.info(s"转换后DataFrame[$index] (${tableConfig.id}) 无数据")
            return 0L
        }
        
        val recordCount = transformedDF.count()
        val description = tableConfig.description.map(d => s" - $d").getOrElse("")
        logger.info(s"转换后DataFrame[$index] (${tableConfig.id}$description) 记录数: $recordCount")
        
        // 条件化调试输出
        val showDebugData = spark.conf.getOption("spark.debug.show.data").getOrElse("false").toBoolean
        if (showDebugData && recordCount > 0) {
            logger.info(s"转换后DataFrame[$index] (${tableConfig.id}) 数据:")
            transformedDF.show(5, truncate = false)
        }
        
        if (recordCount > 0) {
            try {
                // 构建Hudi表名和路径
                val basePath = SparkSessionManager.getStreamingConfigs(spark)("hudiBasePath")
                val hudiTableName = tableConfig.id
                val hudiTablePath = s"$basePath/$hudiTableName"
                
                logger.info(s"开始写入Hudi表: $hudiTableName (${tableConfig.id}, 字段数: ${tableConfig.fieldMapping.size}, 记录数: $recordCount)")
                logger.info(s"Hudi表路径: $hudiTablePath")
                
                // 调用HudiWriter写入数据
                hudiWriter.writeToHudi(transformedDF, hudiTableName, hudiTablePath)
                
                logger.info(s"成功写入Hudi表: $hudiTableName，写入数据量: $recordCount 条")
                
                recordCount
                  
            } catch {
                case e: Exception =>
                    logger.error(s"写入Hudi表时出错 (${tableConfig.id}): ${e.getMessage}", e)
                    throw new RuntimeException(s"写入Hudi表时出错 (${tableConfig.id}): ${e.getMessage}")
            }
        } else {
            0L
        }
    }
} 