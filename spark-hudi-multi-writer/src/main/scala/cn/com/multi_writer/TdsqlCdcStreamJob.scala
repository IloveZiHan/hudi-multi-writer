package cn.com.multi_writer

import cn.com.multi_writer.etl.{DataCleaner, TdsqlBatchProcessor}
import cn.com.multi_writer.sink.HudiWriter
import cn.com.multi_writer.source.KafkaSource
import cn.com.multi_writer.util.SparkJobStatusManager
import cn.com.multi_writer.meta.{MetaHudiTableManager, TableConfig}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.types._

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
 *
 * 配置参数说明：
 * - spark.meta.table.path: 元数据表路径 (默认: /tmp/hudi_tables/meta_hudi_table)
 * - spark.hudi.base.path: Hudi表基础路径 (默认: /tmp/hudi_tables)
 * - spark.debug.show.data: 是否显示调试数据 (默认: false)
 */
object TdsqlCdcStreamJob {

    def main(args: Array[String]): Unit = {
        val spark = SparkSessionManager.createSparkSession("TdSQL CDC Stream Job")

        try {
            println("=== 优化后的TdSQL CDC Stream Job启动 ===")

            // 创建MetaHudiTableManager实例
            val metaManager = new MetaHudiTableManager(spark)

            // 创建相关实例
            val kafkaSource = new KafkaSource(spark)
            val hudiWriter = new HudiWriter(spark)
            val batchProcessor = new TdsqlBatchProcessor(spark)
            val dataCleaner = new DataCleaner(spark)

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
                        , metaManager)
                }
                .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("5 seconds"))
                .start()

            println("流处理已启动，使用动态表配置...")
            query.awaitTermination()

        } catch {
            case e: Exception =>
                println(s"程序执行出错: ${e.getMessage}")
                e.printStackTrace()
        } finally {
            spark.stop()
            println("程序执行完成")
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
                     , metaManager: MetaHudiTableManager): Unit = {
        
        val startTime = System.currentTimeMillis()
        println(s"========== 处理批次 $batchId (动态表配置) ==========")

        val configs = SparkSessionManager.getStreamingConfigs(spark)
        val metaTablePath = configs("metaTablePath")

        if (batchDF.isEmpty) {
            println("本批次无数据")
            return
        }
        
        try {
            // 第一步：从元数据表获取最新的已上线表配置
            val dynamicTableConfigs = getDynamicTableConfigs(metaManager, metaTablePath)
            
            if (dynamicTableConfigs.isEmpty) {
                println("未找到已上线的表配置，跳过本批次处理")
                return
            }

            println(s"动态获取到 ${dynamicTableConfigs.length} 个已上线表配置")

            // 数据清洗
            val cleanedDF = dataCleaner.cleanData(batchDF)
            val cachedDF = cleanedDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
            val cachedCount = cachedDF.count()

            println(s"缓存数据量: $cachedCount 条")

            // 执行数据预处理：使用动态获取的表配置
            transformation(spark,
                cachedDF, 
                batchId, 
                batchProcessor, 
                hudiWriter, 
                dynamicTableConfigs)

            // 释放缓存
            cachedDF.unpersist()

            val endTime = System.currentTimeMillis()
            val processingTime = endTime - startTime
            
            println(s"批次处理完成，耗时: ${processingTime}ms")

        } catch {
            case e: Exception =>
                println(s"批次 $batchId 处理过程中出错: ${e.getMessage}")
                e.printStackTrace()
                throw new RuntimeException(s"批次 $batchId 处理过程中出错: ${e.getMessage}")
        }

        println("========================================")
    }

    /**
     * 从元数据表动态获取表配置信息
     */
    def getDynamicTableConfigs(metaManager: MetaHudiTableManager, metaTablePath: String): Seq[TableConfig] = {
        try {
            println(s"正在从元数据表获取已上线表配置: $metaTablePath")
            
            // 查询状态为1（已上线）的表
            val onlineTablesDF = metaManager.queryTablesByStatus(1, metaTablePath)
            
            if (onlineTablesDF.isEmpty) {
                println("元数据表中没有已上线的表")
                return Seq.empty
            }
            
            val onlineTables = onlineTablesDF.collect()
            println(s"从元数据表获取到 ${onlineTables.length} 个已上线表")
            
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
                
                println(s"处理表配置: $id - $description")
                
                // 解析schema JSON为StructType
                val schema = parseSchemaFromJson(schemaJson)
                
                // 将StructType转换为字段类型映射
                val fieldMappings = schema.fields.map(field => (field.name, field.dataType)).toSeq
                
                println(s"表 $id 包含 ${fieldMappings.length} 个字段")
                
                TableConfig(id, fieldMappings, status, isPartitioned, Some(tags), Some(description), Some(sourceDb), Some(sourceTable), Some(dbType), Some(partitionExpr), Some(hoodieConfig))   
            }.toSeq
            
            tableConfigs
            
        } catch {
            case e: Exception =>
                println(s"获取动态表配置失败: ${e.getMessage}")
                e.printStackTrace()
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
                println(s"解析schema JSON失败: ${e.getMessage}")
                println(s"Schema JSON: ${schemaJson.take(200)}...")
                throw e
        }
    }


    /**
     * 使用动态表配置的数据预处理执行函数
     */
    private def transformation(spark: SparkSession,
                                                   cachedDF: DataFrame,
                                                   batchId: Long,
                                                   batchProcessor: TdsqlBatchProcessor,
                                                   hudiWriter: HudiWriter,
                                                   dynamicTableConfigs: Seq[TableConfig]): Unit = {
        try {
            println("开始执行数据预处理（动态表配置）...")
            println(s"使用动态表配置数量: ${dynamicTableConfigs.length}")
            
            // 使用batchTransform方法进行数据预处理
            val transformedDataFrames = batchProcessor.batchTransform(cachedDF, dynamicTableConfigs)
            
            println(s"数据预处理完成，转换后DataFrame数量: ${transformedDataFrames.length}")
            
            // 批量处理转换后的DataFrame
            val processResults = transformedDataFrames.zip(dynamicTableConfigs).zipWithIndex.map { 
                case ((transformedDF, tableConfig), index) =>
                    processTransformedDataFrameWithDynamic(spark, transformedDF, batchId, tableConfig,
                                                          index, hudiWriter)
            }
            
            val totalWrittenRecords = processResults.sum
            println(s"批量处理完成，总写入记录数: $totalWrittenRecords")
            
        } catch {
            case e: Exception =>
                println(s"数据预处理过程中出错: ${e.getMessage}")
                e.printStackTrace()
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
            println(s"转换后DataFrame[$index] (${tableConfig.id}) 无数据")
            return 0L
        }
        
        val recordCount = transformedDF.count()
        val description = tableConfig.description.map(d => s" - $d").getOrElse("")
        println(s"转换后DataFrame[$index] (${tableConfig.id}$description) 记录数: $recordCount")
        
        // 条件化调试输出
        val showDebugData = spark.conf.getOption("spark.debug.show.data").getOrElse("false").toBoolean
        if (showDebugData && recordCount > 0) {
            println(s"转换后DataFrame[$index] (${tableConfig.id}) 数据:")
            transformedDF.show(5, truncate = false)
        }
        
        if (recordCount > 0) {
            try {
                // 构建Hudi表名和路径
                val basePath = SparkSessionManager.getStreamingConfigs(spark)("hudiBasePath")
                val hudiTableName = tableConfig.id
                val hudiTablePath = s"$basePath/$hudiTableName"
                
                println(s"开始写入Hudi表: $hudiTableName (${tableConfig.id}, 字段数: ${tableConfig.fieldMapping.size}, 记录数: $recordCount)")
                println(s"Hudi表路径: $hudiTablePath")
                
                // 调用HudiWriter写入数据
                hudiWriter.writeToHudi(transformedDF, hudiTableName, hudiTablePath)
                
                println(s"成功写入Hudi表: $hudiTableName，写入数据量: $recordCount 条")
                
                recordCount
                  
            } catch {
                case e: Exception =>
                    println(s"写入Hudi表时出错 (${tableConfig.id}): ${e.getMessage}")
                    e.printStackTrace()
                    throw new RuntimeException(s"写入Hudi表时出错 (${tableConfig.id}): ${e.getMessage}")
            }
        } else {
            0L
        }
    }
} 