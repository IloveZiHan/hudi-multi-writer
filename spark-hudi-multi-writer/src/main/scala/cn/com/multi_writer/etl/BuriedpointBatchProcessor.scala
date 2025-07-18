package cn.com.multi_writer.etl

import cn.com.multi_writer.meta.TableConfig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

/**
 * 埋点数据微批处理预处理器
 * 用于将从Kafka消费的埋点数据DataFrame预处理为待写入Hudi表的DataFrame
 *
 * 功能包括：
 * 1. 标准化埋点数据格式
 * 2. 过滤指定表的数据
 * 3. 字段映射和类型转换
 * 4. 数据质量检查和清洗
 * 5. 提供统一的数据转换接口
 */
class BuriedpointBatchProcessor(spark: SparkSession) {

    private val logger: Logger = LoggerFactory.getLogger(classOf[BuriedpointBatchProcessor])
    
    // 导入隐式转换
    import spark.implicits._

    /**
     * 过滤指定表的埋点数据
     * 根据表配置过滤出对应的数据
     *
     * @param batchDF 输入的DataFrame
     * @param tableConfig 表配置信息
     * @return 过滤后的DataFrame
     */
    def filterByTableConfig(batchDF: DataFrame, tableConfig: TableConfig): DataFrame = {
        logger.info(s"开始过滤表 ${tableConfig.id} 的埋点数据...")
        
        // 从表配置中获取过滤条件
        val sourceTable = tableConfig.sourceTable.getOrElse("")
        val tags = tableConfig.tags.getOrElse("")
        
        var filteredDF = batchDF
        
        // 根据源表名过滤
        if (sourceTable.nonEmpty) {
            filteredDF = filteredDF.filter(col("parsed_data.dcBpTable") === sourceTable)
        }
        
        val count = filteredDF.count()
        logger.info(s"表 ${tableConfig.id} 过滤后数据量: $count 条")
        
        filteredDF
    }

    /**
     * 构建CDC字段表达式
     * 为埋点数据添加CDC相关字段
     * 
     * @param partitionExpr 分区表达式
     * @return CDC字段表达式序列
     */
    def buildCdcFields(partitionExpr: String): Seq[Column] = {
        // 构建 cdc_dt 字段表达式
        val cdcDtExpression = if (partitionExpr.isEmpty) {
            // 默认使用事件时间作为分区
            when(col("dcBpEventTime").isNotNull, 
                date_format(col("dcBpEventTime"), "yyyy-MM-dd"))
            .otherwise(date_format(current_timestamp(), "yyyy-MM-dd"))
            .alias("cdc_dt")
        } else {
            expr(partitionExpr).cast(StringType).alias("cdc_dt")
        }
        
        // 构建 cdc_delete_flag 字段表达式
        // 埋点数据通常不涉及删除操作，默认为0
        val cdcDeleteFlagExpression = lit(0).alias("cdc_delete_flag")
        
        Seq(cdcDtExpression, cdcDeleteFlagExpression)
    }

    /**
     * 提取并转换字段
     * 根据字段映射提取埋点数据中的字段并进行类型转换
     *
     * @param batchDF 输入的DataFrame
     * @param fieldTypeMappings 字段名到数据类型的映射列表
     * @param partitionExpr 分区表达式
     * @return 转换后的DataFrame
     */
    def extractAndConvertFields(batchDF: DataFrame, 
                               fieldTypeMappings: Seq[(String, DataType)], 
                               partitionExpr: String): DataFrame = {
        
        logger.info(s"开始提取和转换字段，字段数量: ${fieldTypeMappings.length}")
        
        // 将字段映射转换为Spark表达式列表
        val fieldExpressions = fieldTypeMappings.map { case (fieldName, dataType) =>
            // fieldName是下划线命名需要转换为驼峰式命名
            val camelFieldName = fieldName.split("_").map(_.capitalize).mkString("")
            // 将首字母设置为小写
            val lowerCamelFieldName = camelFieldName.substring(0, 1).toLowerCase + camelFieldName.substring(1)  
            println(s"camelFieldName: $camelFieldName,lowerCamelFieldName: $lowerCamelFieldName")
            col(s"parsed_data.${lowerCamelFieldName}").cast(dataType).alias(fieldName)
        }

        // 构建CDC字段表达式
        val cdcFieldExpressions = buildCdcFields(partitionExpr)

        // 组合所有字段表达式
        val allExpressions = fieldExpressions ++ cdcFieldExpressions

        val resultDF = batchDF.select(allExpressions: _*)
        
        logger.info("字段提取和类型转换完成")
        resultDF
    }


    /**
     * 统一的数据转换接口
     * 将所有预处理步骤组合在一起
     *
     * @param batchDF 输入的批处理DataFrame
     * @param tableConfig 表配置信息
     * @return 完全处理后的DataFrame，可直接写入Hudi
     */
    def transform(batchDF: DataFrame, tableConfig: TableConfig): DataFrame = {
        logger.info(s"开始转换埋点数据 - 表: ${tableConfig.id}")

        try {
            // 过滤指定表的数据
            val filteredDF = filterByTableConfig(batchDF, tableConfig)
            logger.info("表数据过滤完成")

            val filteredCount = filteredDF.count()
            if (filteredCount > 0) {
                // 提取字段并进行类型转换
                val partitionExpr = tableConfig.partitionExpr.getOrElse("'default'")
                val convertedDF = extractAndConvertFields(filteredDF, tableConfig.fieldMapping, partitionExpr)
                logger.info("字段提取和类型转换完成")

                // 检查转换结果
                val finalCount = convertedDF.count()
                logger.info(s"埋点数据转换完成 - 最终数据量: $finalCount 条")

                if (finalCount > 0) {
                    logger.info("转换后的数据样例:")
                    convertedDF.show(3, truncate = false)
                } else {
                    logger.warn("警告: 字段提取后数据为空，可能存在字段匹配问题")
                }

                convertedDF
            } else {
                logger.info("没有匹配的数据需要转换")
                // 返回空的DataFrame，仅包含指定字段和附加字段的schema结构
                val emptySchema = StructType(
                    tableConfig.fieldMapping.map { case (fieldName, dataType) => 
                        StructField(fieldName, dataType, nullable = true) 
                    }
                )
                spark.createDataFrame(spark.sparkContext.emptyRDD[org.apache.spark.sql.Row], emptySchema)
            }
        } catch {
            case e: Exception =>
                logger.error(s"转换埋点数据时出错: ${e.getMessage}", e)
                throw e
        }
    }

    /**
     * 批量转换多个表的埋点数据
     *
     * @param batchDF 输入的批处理DataFrame
     * @param tableConfigs 表配置列表
     * @return 转换后的DataFrame列表
     */
    def batchTransform(batchDF: DataFrame, tableConfigs: Seq[TableConfig]): Seq[DataFrame] = {
        logger.info(s"开始批量转换埋点数据，表配置数量: ${tableConfigs.length}")
        
        val results = tableConfigs.map { tableConfig =>
            transform(batchDF, tableConfig)
        }.filter(_.count() > 0) // 过滤掉空的DataFrame
        
        logger.info(s"批量转换完成，生成有效DataFrame数量: ${results.length}")
        results
    }
}

/**
 * BuriedpointBatchProcessor伴生对象
 */
object BuriedpointBatchProcessor {

    /**
     * 创建BuriedpointBatchProcessor实例
     *
     * @param spark SparkSession实例
     * @return BuriedpointBatchProcessor实例
     */
    def apply(spark: SparkSession): BuriedpointBatchProcessor = {
        new BuriedpointBatchProcessor(spark)
    }
} 