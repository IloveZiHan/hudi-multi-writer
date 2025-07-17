package cn.com.multi_writer.etl

import cn.com.multi_writer.meta.TableConfig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

/**
 * Canal批处理器
 * 用于处理Canal binlog消息并转换为目标表格式的DataFrame
 */
class CanalBatchProcessor(spark: SparkSession) {
    
    private val logger: Logger = LoggerFactory.getLogger(getClass)
    
    /**
     * 批量转换Canal数据
     * @param inputDF 输入的Canal解析后的DataFrame
     * @param tableConfigs 表配置列表
     * @return 转换后的DataFrame列表
     */
    def batchTransform(inputDF: DataFrame, tableConfigs: Seq[TableConfig]): Seq[DataFrame] = {
        logger.info(s"开始批量转换Canal数据，表配置数量: ${tableConfigs.length}")
        
        val results = tableConfigs.map { tableConfig =>
            transformForTable(inputDF, tableConfig)
        }
        
        logger.info(s"批量转换完成，生成DataFrame数量: ${results.length}")
        results
    }
    
    /**
     * 为指定表配置转换数据
     * @param inputDF 输入的Canal解析后的DataFrame
     * @param tableConfig 表配置
     * @return 转换后的DataFrame
     */
    private def transformForTable(inputDF: DataFrame, tableConfig: TableConfig): DataFrame = {
        try {
            logger.info(s"开始转换表: ${tableConfig.id}")
            
            // 从表配置中获取源数据库和表名
            val sourceDb = tableConfig.sourceDb.getOrElse("")
            val sourceTable = tableConfig.sourceTable.getOrElse("")
            
            if (sourceDb.isEmpty || sourceTable.isEmpty) {
                logger.warn(s"表配置 ${tableConfig.id} 缺少源数据库或表名信息")
                return spark.emptyDataFrame
            }
            
            // 过滤出当前表的数据
            val filteredDF = inputDF.filter(
                col("database") === sourceDb && 
                col("table") === sourceTable &&
                col("is_ddl") === false
            )
            
            if (filteredDF.isEmpty) {
                logger.info(s"表 ${tableConfig.id} 在当前批次中无数据")
                return spark.emptyDataFrame
            }
            
            // 展开data数组，每个记录成为一行
            val expandedDF = filteredDF
                .select(
                    col("database"),
                    col("table"),
                    col("event_type"),
                    col("timestamp"),
                    col("data"),
                    col("old"),
                    col("pk_names"),
                    col("mysql_type"),
                    col("sql_type")
                )
                .select(
                    col("database"),
                    col("table"),
                    col("event_type"),
                    col("timestamp"),
                    col("pk_names"),
                    col("mysql_type"),
                    col("sql_type"),
                    posexplode(col("data")).as(Seq("pos", "parsed_data"))
                )

            // 数据已经是解析后的Map格式，直接使用
            val parsedDF = expandedDF
            parsedDF.show
            
            // 转换为目标表格式
            val transformedDF = transformToTargetFormat(parsedDF, tableConfig)
            
            logger.info(s"表 ${tableConfig.id} 转换完成")
            transformedDF.show
            transformedDF
            
        } catch {
            case e: Exception =>
                logger.error(s"转换表 ${tableConfig.id} 数据时出错: ${e.getMessage}", e)
                spark.emptyDataFrame
        }
    }
    
    /**
     * 转换为目标表格式
     * @param parsedDF 解析后的DataFrame
     * @param tableConfig 表配置
     * @return 目标格式的DataFrame
     */
    private def transformToTargetFormat(parsedDF: DataFrame, tableConfig: TableConfig): DataFrame = {
        try {

            val fieldExpressions = tableConfig.fieldMapping.zipWithIndex.map { case ((fieldName, dataType), index) =>
                col("parsed_data").getItem(fieldName).cast(dataType).alias(fieldName)
            }.toList


            // 构建CDC字段表达式
            val cdcFieldExpressions = buildCdcFields(tableConfig.partitionExpr.get)

            
            val allExpressions = fieldExpressions ++ cdcFieldExpressions
            
            // 应用转换
            val frame = parsedDF.select(allExpressions: _*)
            frame.show

            
            logger.info(s"目标表 ${tableConfig.id} 格式转换完成，字段数: ${tableConfig.fieldMapping.length}")
            frame
            
        } catch {
            case e: Exception =>
                logger.error(s"转换目标表格式时出错: ${e.getMessage}", e)
                spark.emptyDataFrame
        }
    }

    /**
     * 构建CDC字段表达式
     *
     * @param partitionExpr 分区表达式，为空时使用默认值
     * @return CDC字段表达式序列，包含 cdc_dt 和 cdc_delete_flag
     */
    def buildCdcFields(partitionExpr: String): Seq[Column] = {
        // 构建 cdc_dt 字段表达式
        val cdcDtExpression = if (partitionExpr.isEmpty) {
            expr("'default'").cast(StringType).alias("cdc_dt")
        } else {
            expr(partitionExpr).cast(StringType).alias("cdc_dt")
        }

        // 构建 cdc_delete_flag 字段表达式
        val cdcDeleteFlagExpression = when(lower(col("event_type")) === "delete", lit(1))
            .otherwise(lit(0))
            .alias("cdc_delete_flag")

        Seq(cdcDtExpression, cdcDeleteFlagExpression)
    }
    
    /**
     * 将列转换为指定的数据类型
     * @param sourceCol 源列
     * @param targetType 目标数据类型
     * @param fieldName 字段名
     * @return 转换后的列
     */
    private def castColumnToType(sourceCol: org.apache.spark.sql.Column, 
                                targetType: DataType, 
                                fieldName: String): org.apache.spark.sql.Column = {
        targetType match {
            case IntegerType => 
                when(sourceCol.isNotNull && sourceCol =!= "", sourceCol.cast(IntegerType))
                    .otherwise(lit(null).cast(IntegerType))
            
            case LongType => 
                when(sourceCol.isNotNull && sourceCol =!= "", sourceCol.cast(LongType))
                    .otherwise(lit(null).cast(LongType))
            
            case DoubleType => 
                when(sourceCol.isNotNull && sourceCol =!= "", sourceCol.cast(DoubleType))
                    .otherwise(lit(null).cast(DoubleType))
            
            case FloatType => 
                when(sourceCol.isNotNull && sourceCol =!= "", sourceCol.cast(FloatType))
                    .otherwise(lit(null).cast(FloatType))
            
            case BooleanType => 
                when(sourceCol.isNotNull && sourceCol =!= "", sourceCol.cast(BooleanType))
                    .otherwise(lit(null).cast(BooleanType))
            
            case TimestampType => 
                when(sourceCol.isNotNull && sourceCol =!= "", 
                    to_timestamp(sourceCol, "yyyy-MM-dd HH:mm:ss"))
                    .otherwise(lit(null).cast(TimestampType))
            
            case DateType => 
                when(sourceCol.isNotNull && sourceCol =!= "", 
                    to_date(sourceCol, "yyyy-MM-dd"))
                    .otherwise(lit(null).cast(DateType))
            
            case _: DecimalType =>
                when(sourceCol.isNotNull && sourceCol =!= "", sourceCol.cast(targetType))
                    .otherwise(lit(null).cast(targetType))
            
            case StringType => 
                when(sourceCol.isNotNull, sourceCol.cast(StringType))
                    .otherwise(lit(null).cast(StringType))
            
            case _ => 
                logger.warn(s"未知的数据类型: $targetType for field: $fieldName，使用字符串类型")
                when(sourceCol.isNotNull, sourceCol.cast(StringType))
                    .otherwise(lit(null).cast(StringType))
        }
    }
    
    /**
     * 过滤指定操作类型的数据
     * @param inputDF 输入DataFrame
     * @param eventTypes 事件类型列表（INSERT, UPDATE, DELETE）
     * @return 过滤后的DataFrame
     */
    def filterByEventType(inputDF: DataFrame, eventTypes: Seq[String]): DataFrame = {
        inputDF.filter(col("event_type").isin(eventTypes: _*))
    }
    
    /**
     * 过滤指定数据库和表的数据
     * @param inputDF 输入DataFrame
     * @param database 数据库名
     * @param table 表名
     * @return 过滤后的DataFrame
     */
    def filterByTable(inputDF: DataFrame, database: String, table: String): DataFrame = {
        inputDF.filter(col("database") === database && col("table") === table)
    }
}