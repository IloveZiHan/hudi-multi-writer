package cn.com.multi_writer

import cn.com.multi_writer.source.KafkaSource
import cn.com.multi_writer.util.SparkJobStatusManager
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.types._

/**
 * TdSQL CDC 流处理作业
 *
 * 重构后的版本特性：
 * 1. 基于项目中已有的 KafkaSource、KafkaSchema 和 SparkSessionManager 实现
 * 2. 所有配置参数通过Spark配置传入，而不是命令行参数
 * 3. Kafka消费和数据解析逻辑完全封装在KafkaSource中
 * 4. 使用KafkaSchema中定义的TdSQL Binlog结构
 * 5. 统一使用SparkSessionManager管理SparkSession
 * 6. 集成HudiWriter实现数据写入Hudi表功能
 * 7. 集成TdsqlBatchProcessor实现数据预处理功能
 *
 * 配置参数说明：
 * - spark.kafka.bootstrap.servers: Kafka服务器地址 (默认: 10.94.162.31:9092)
 * - spark.kafka.topic: Kafka主题名称 (默认: rtdw_tdsql_alc) 
 * - spark.shuffle_partition.records: 每分区目标记录数，用于动态计算分区数 (默认: 100000)
 * - spark.hudi.base.path: Hudi表基础路径 (默认: /tmp/hudi_tables)
 * - spark.hudi.target.tables: 目标表配置，格式：db1.table1:/path1,db2.table2:/path2
 *
 * 使用示例:
 * spark-submit \
 *   --conf spark.kafka.bootstrap.servers=10.94.162.31:9092 \
 *   --conf spark.kafka.topic=rtdw_tdsql_alc \
 *   --conf spark.shuffle_partition.records=50000 \
 *   --conf spark.hudi.base.path=/tmp/hudi_tables \
 *   --conf spark.hudi.target.tables=test_db.user_table:/tmp/hudi_tables/test_db_user_table \
 *   your-jar-file.jar
 */
object TdsqlCdcStreamJob {

    def main(args: Array[String]): Unit = {
        // 使用SparkSessionManager创建SparkSession
        val spark = SparkSessionManager.createSparkSession("TdSQL CDC Stream Job")

        try {
            println("=== TdSQL CDC 流处理作业启动 ===")

            // 使用SparkSessionManager获取流处理配置
            SparkSessionManager.printStreamingConfigDetails(spark)

            val configs = SparkSessionManager.getStreamingConfigs(spark)

            // 创建KafkaSource实例
            val kafkaSource = new KafkaSource(spark)

            // 创建HudiWriter实例
            val hudiWriter = new HudiWriter(spark)

            // 创建TdsqlBatchProcessor实例
            val batchProcessor = new TdsqlBatchProcessor(spark)

            // 使用KafkaSource创建完整的解析后数据流
            val parsedStream = kafkaSource.createParsedStream()

            println("Kafka数据流创建成功，开始启动流处理...")

            // 启动流处理查询，使用foreachBatch进行微批处理
            val query = parsedStream.writeStream
                .outputMode("append")
                .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
                    processBatch(spark, batchDF, batchId, kafkaSource, hudiWriter, batchProcessor)
                }
                .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("5 seconds")) // 5秒触发一次
                .start()

            println("流处理已启动，使用foreachBatch进行微批处理...")
            println("触发间隔: 5秒")
            println("按 Ctrl+C 停止程序")

            // 等待流处理完成
            query.awaitTermination()

        } catch {
            case e: Exception =>
                println(s"程序执行出错: ${e.getMessage}")
                e.printStackTrace()
        } finally {
            // 使用SparkSessionManager停止SparkSession
            cn.com.multi_writer.SparkSessionManager.stopSparkSession(spark)
            println("程序执行完成")
        }
    }

    /**
     * 创建基于table_schema.json的表配置，用于测试数据预处理功能
     * 
     * @return 表配置列表：Seq[(数据库名, 表名, 字段类型映射)]
     */
    def createTestTableConfigs(): Seq[(String, String, Seq[(String, DataType)])] = {
        // 基于table_schema.json中loan_info_table的核心字段创建字段映射
        val loanTableFieldMappings = Seq(
            // 基本字段
            ("id", StringType),                       // 物理主键
            ("loan_no", StringType),                  // 借据号
            ("cont_no", StringType),                  // 合同号
            ("cust_no", StringType),                  // 客户编号
            ("cust_name", StringType),                // 客户姓名
            ("id_type", StringType),                  // 证件类型
            ("id_no", StringType),                    // 证件号码
            
            // 产品相关字段
            ("prod_no", StringType),                  // 产品编号
            ("prod_id", StringType),                  // 产品ID
            ("trade_dt", StringType),                 // 交易日期
            ("loan_sts", StringType),                 // 借据状态
            
            // 日期相关字段
            ("int_start_dt", StringType),             // 起息日
            ("due_dt", StringType),                   // 到期日
            ("fst_repay_dt", StringType),             // 第一次扣款日
            ("last_repay_dt", StringType),            // 上次还款日
            ("curr_repay_dt", StringType),            // 当前还款日
            ("next_repay_dt", StringType),            // 下一还款日
            
            // 数值型字段
            ("loan_term", IntegerType),               // 贷款期限
            ("repay_dt", IntegerType),                // 还款日
            ("repay_intrv", IntegerType),             // 还款间隔
            ("loan_tnr", IntegerType),                // 贷款期数
            ("grace_perd_days", IntegerType),         // 宽限期天数
            
            // 金额字段
            ("int_rate", DoubleType),                 // 执行利率
            ("od_int_rate", DoubleType),              // 罚息利率
            ("loan_amt", DoubleType),                 // 贷款金额
            ("loan_bal", DoubleType),                 // 贷款余额
            ("vat_tax_rate", DoubleType),             // 增值税税率
            ("our_pty_contri_ratio", DoubleType),     // 我方出资比例
            ("loan_bal_accum", DoubleType),           // 借据余额积数
            ("comb_rate", DoubleType),                // 综合年化利率
            
            // 业务字段
            ("repay_intrv_unit", StringType),         // 还款间隔单位
            ("repay_mode_code", StringType),          // 还款方式代码
            ("instm_ind", StringType),                // 期供标志
            ("loan_account_sts", StringType),         // 借据账务状态
            ("five_lvl_class_code", StringType),      // 五级分类编码
            ("wrtoff_dt", StringType),                // 核销日期
            ("grace_perd_type", StringType),          // 宽限期类型
            ("calc_od_int_part", StringType),         // 计算罚息部分
            ("loan_deflt_ind", StringType),           // 贷款拖欠标志
            
            // 系统字段
            ("create_time", StringType),              // 创建时间
            ("create_user", StringType),              // 创建人
            ("update_time", StringType),              // 修改时间
            ("update_user", StringType),              // 修改人
            ("del_flag", StringType),                 // 逻辑删标志
            
            // ETL相关字段
            ("src_sysname", StringType),              // 来源源系统代码
            ("src_table", StringType),                // 来源源系统表名
            ("etl_tx_dt", StringType),                // ETL业务日期
            ("etl_first_dt", StringType),             // ETL首次插入日期
            ("etl_proc_dt", StringType),              // ETL处理时间戳
            ("start_dt", StringType),                 // 开始日期
            
            // 业务扩展字段
            ("account_org", StringType),              // 账务机构
            ("chan_no", StringType),                  // 进件渠道
            ("prft_type", StringType),                // 收益类型
            ("unite_crdt_ind", StringType),           // 联合贷标志
            ("insure_ind", StringType),               // 承保标志
            ("portfolio_code", StringType),           // 资产池编号
            ("confirmation_type", StringType),        // 终止确认类型
            ("fund_type", StringType),                // 放款类型
            ("amort_loan_no", StringType),            // 账单分期借据号
            ("cdc_delete_flag", StringType)           // CDC删除标志
        )

        // 返回表配置列表，这里测试使用test_db数据库下的loan_info_table表
        Seq(
            ("alc", "t_alc_loan", loanTableFieldMappings)
        )
    }

    /**
     * 处理微批次数据
     * 集成了KafkaSource的数据清洗功能、TdsqlBatchProcessor的数据预处理功能和HudiWriter的写入功能
     *
     * @param spark SparkSession实例
     * @param batchDF 批次DataFrame
     * @param batchId 批次ID
     * @param kafkaSource KafkaSource实例，用于数据清洗
     * @param hudiWriter HudiWriter实例，用于写入Hudi表
     * @param batchProcessor TdsqlBatchProcessor实例，用于数据预处理
     */
    def processBatch(spark: SparkSession, batchDF: DataFrame, batchId: Long,
                     kafkaSource: KafkaSource, hudiWriter: HudiWriter, 
                     batchProcessor: TdsqlBatchProcessor): Unit = {
        println(s"========== 处理批次 $batchId ==========")

        // 统计总数据量
        val totalCount = batchDF.count()
        println(s"总数据量: $totalCount 条")

        // 设置批次开始处理状态到WebUI
        SparkJobStatusManager.setProcessingStageStatus(spark, batchId, "批次开始", s"开始处理批次 $batchId，数据量: $totalCount 条")

        if (totalCount > 0) {
            // 使用KafkaSource的数据清洗方法
            val cleanedDF = kafkaSource.cleanData(batchDF)

            // 设置数据清洗完成状态到WebUI
            val cleanedCount = cleanedDF.count()
            SparkJobStatusManager.setProcessingStageStatus(spark, batchId, "数据清洗", s"数据清洗完成，清洗后数据量: $cleanedCount 条")

            // 对清洗后的数据进行shuffle和cache处理
            val shuffledAndCachedDF = shuffleAndCacheData(spark, cleanedDF)

            // 处理清洗后的数据并设置统计信息到WebUI
            processCleanedDataWithJobStatus(spark, shuffledAndCachedDF, batchId)

            // 执行数据预处理：使用TdsqlBatchProcessor的batchTransform方法
            try {
                println("开始执行数据预处理...")
                
                // 创建测试用的表配置
                val tableConfigs = createTestTableConfigs()
                println(s"创建表配置成功，配置数量: ${tableConfigs.length}")
                
                // 使用batchTransform方法进行数据预处理
                val transformedDataFrames = batchProcessor.batchTransform(shuffledAndCachedDF, tableConfigs)
                
                println(s"数据预处理完成，转换后DataFrame数量: ${transformedDataFrames.length}")
                
                // 设置数据预处理完成状态到WebUI
                SparkJobStatusManager.setProcessingStageStatus(spark, batchId, "数据预处理", 
                  s"数据预处理完成，转换后DataFrame数量: ${transformedDataFrames.length}")
                
                // 对每个转换后的DataFrame进行处理
                transformedDataFrames.zipWithIndex.foreach { case (transformedDF, index) =>
                    val recordCount = transformedDF.count()
                    println(s"转换后DataFrame[$index] 记录数: $recordCount")
                    
                    if (recordCount > 0) {
                        // 显示转换后的数据结构信息
                        println(s"转换后DataFrame[$index] 数据:")
                        transformedDF.show()
                        
                        // 这里可以根据需要将转换后的数据写入Hudi表
                        // hudiWriter.writeToHudi(transformedDF, targetPath, tableName)
                        
                        println(s"转换后DataFrame[$index] 处理完成")
                    } else {
                        println(s"转换后DataFrame[$index] 无数据")
                    }
                }
                
            } catch {
                case e: Exception =>
                    println(s"数据预处理过程中出错: ${e.getMessage}")
                    e.printStackTrace()
                    // 设置预处理错误状态到WebUI
                    SparkJobStatusManager.setProcessingStageStatus(spark, batchId, "预处理错误", s"数据预处理失败: ${e.getMessage}")
            }
            

            // 释放缓存资源
            shuffledAndCachedDF.unpersist()

            // 设置批次完成状态到WebUI
            SparkJobStatusManager.setProcessingStageStatus(spark, batchId, "批次完成", s"批次 $batchId 处理完成，处理数据量: $totalCount 条")

            println("批次处理完成")
        } else {
            // 设置空批次状态到WebUI
            SparkJobStatusManager.setProcessingStageStatus(spark, batchId, "空批次", s"批次 $batchId 无数据")
            println("本批次无数据")
        }

        println(s"========================================")
        println()
    }

    /**
     * 对数据进行shuffle和cache处理
     * 优化数据分布，提高后续处理性能
     * 根据数据量动态计算分区数
     *
     * @param spark SparkSession实例，用于获取配置
     * @param df 待处理的DataFrame
     * @return shuffle和cache后的DataFrame
     */
    def shuffleAndCacheData(spark: SparkSession, df: DataFrame): DataFrame = {
        // 获取原始数据量，用于计算分区数
        val originalCount = df.count()
        println(s"原始数据量: $originalCount 条")

        // 使用SparkSessionManager获取配置
        val configs = SparkSessionManager.getStreamingConfigs(spark)
        val recordsPerPartition = configs("recordsPerPartition").toLong

        // 计算分区数，最小为1
        val partitionCount = math.max(1, (originalCount / recordsPerPartition).toInt)
        println(s"计算得出的分区数: $partitionCount")

        // 使用不同的shuffle策略
        val repartitionedDF = if (originalCount > 10000) {
            // 对于大数据量，添加随机列来改善分区均匀性
            df.withColumn("__shuffle_key__", (rand() * partitionCount).cast("int"))
                .repartition(partitionCount, col("__shuffle_key__"))
                .drop("__shuffle_key__")
        } else {
            // 对于小数据量，直接使用repartition
            df.repartition(partitionCount)
        }

        // 使用MEMORY_AND_DISK_SER存储级别进行缓存
        val cachedDF = repartitionedDF.persist(StorageLevel.MEMORY_AND_DISK_SER)

        // 触发一次action操作来实际执行repartition和cache
        val recordCount = cachedDF.count()
        println(s"shuffle和cache处理完成，记录数: $recordCount")

        cachedDF
    }

    /**
     * 处理清洗后的数据并将统计信息设置到WebUI
     * 使用SparkJobStatusManager替代show()方法
     * 基于TdSQL Binlog的特定字段进行数据分析和处理
     *
     * @param spark SparkSession实例
     * @param df 清洗后的DataFrame
     * @param batchId 批次ID
     */
    def processCleanedDataWithJobStatus(spark: SparkSession, df: DataFrame, batchId: Long): Unit = {

        try {
            // 获取事件类型统计信息并设置到WebUI
            val eventTypeStats = SparkJobStatusManager.getEventTypeStats(df)
            SparkJobStatusManager.setProcessingStageStatus(spark, batchId, "事件类型统计", eventTypeStats)

            // 获取表操作统计信息并设置到WebUI
            val tableOperationStats = SparkJobStatusManager.getTableOperationStats(df)
            SparkJobStatusManager.setProcessingStageStatus(spark, batchId, "表操作统计", tableOperationStats)

            // 获取Kafka元数据统计信息并设置到WebUI
            val kafkaMetadataStats = SparkJobStatusManager.getKafkaMetadataStats(df)
            SparkJobStatusManager.setProcessingStageStatus(spark, batchId, "Kafka元数据统计", kafkaMetadataStats)

            // 将完整的批次处理统计信息设置到WebUI
            val totalCount = df.count()
            SparkJobStatusManager.setBatchProcessingStatus(
                spark,
                batchId,
                totalCount,
                eventTypeStats,
                tableOperationStats,
                kafkaMetadataStats
            )

        } catch {
            case e: Exception =>
                println(s"数据处理过程中出错: ${e.getMessage}")
                e.printStackTrace()
                // 设置错误状态到WebUI
                SparkJobStatusManager.setProcessingStageStatus(spark, batchId, "处理错误", s"数据处理失败: ${e.getMessage}")
        }
    }

} 