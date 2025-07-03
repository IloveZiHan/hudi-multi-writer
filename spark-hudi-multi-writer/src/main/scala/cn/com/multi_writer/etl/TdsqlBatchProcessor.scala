package cn.com.multi_writer.etl

import cn.com.multi_writer.meta.TableConfig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

/**
 * TdSQL微批处理预处理器
 * 用于将从Kafka消费的DataFrame预处理为待写入Hudi表的DataFrame
 *
 * 功能包括：
 * 1. 根据事件类型处理数据字段（insert/update使用field，delete使用where）
 * 2. 过滤指定数据库和表名的数据
 * 3. 获取待写入Hudi的字段并进行类型转换
 * 4. 提供统一的数据转换接口
 */
class TdsqlBatchProcessor(spark: SparkSession) {

    private val logger: Logger = LoggerFactory.getLogger(classOf[TdsqlBatchProcessor])
    
    // 导入隐式转换

    /**
     * 根据事件类型处理数据字段
     *
     * @param batchDF 输入的批处理DataFrame
     * @return 处理后的DataFrame，包含统一的数据字段
     */
    def processDataFieldsByEventType(batchDF: DataFrame): DataFrame = {
        // 创建UDF函数来根据事件类型选择对应的数据字段
        val selectDataFieldsUDF = udf((eventTypeStr: String, fieldArray: Seq[String], whereArray: Seq[String]) => {
            if (eventTypeStr != null) {
                eventTypeStr.toLowerCase match {
                    case "insert" | "update" =>
                        // insert和update事件使用field字段
                        fieldArray
                    case "delete" =>
                        // delete事件使用where字段
                        whereArray
                    case _ =>
                        // 其他事件类型返回null，后续会被过滤掉
                        null
                }
            } else {
                null
            }
        })

        // 应用字段选择逻辑
        batchDF
            .withColumn("data_fields", selectDataFieldsUDF(col("eventtypestr"), col("field"), col("where")))
            // 过滤掉不支持的事件类型
            .filter(col("data_fields").isNotNull)
            // 过滤掉空的数据字段
            .filter(size(col("data_fields")) > 0)
    }

    /**
     * 过滤指定数据库和表名的数据
     *
     * @param batchDF 输入的DataFrame
     * @param dbName 目标数据库名
     * @param tableName 目标表名
     * @return 过滤后的DataFrame
     */
    def filterByDatabaseAndTable(batchDF: DataFrame, dbName: String, tableName: String): DataFrame = {
        batchDF
            .filter(col("db") === dbName)
            .filter(col("table") === tableName)
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
        val cdcDeleteFlagExpression = when(lower(col("eventtypestr")) === "delete", lit(1))
            .otherwise(lit(0))
            .alias("cdc_delete_flag")
        
        Seq(cdcDtExpression, cdcDeleteFlagExpression)
    }

    /**
     * 获取所有待写入Hudi的字段并进行类型转换
     * 优化后仅包含fieldTypeMappings中的字段以及cdc_dt、cdc_delete_flag两个附加字段
     *
     * @param batchDF 输入的DataFrame
     * @param fieldTypeMappings 字段名到数据类型的映射列表，格式：Seq((字段名, 数据类型))
     * @return 转换后的DataFrame，仅包含指定字段和附加字段
     */
    def extractAndConvertFields(batchDF: DataFrame
                            , fieldTypeMappings: Seq[(String, DataType)]
                            , partitionExpr: String): DataFrame = {

        // 将字段列表转换为Spark表达式列表
        val fieldExpressions = fieldTypeMappings.zipWithIndex.map { case ((fieldName, dataType), index) =>
            col("data_fields").getItem(index).cast(dataType).alias(fieldName)
        }.toList

        // 构建CDC字段表达式
        val cdcFieldExpressions = buildCdcFields(partitionExpr)

        // 添加CDC字段
        val allExpressions = fieldExpressions ++ cdcFieldExpressions

        val frame = batchDF.select(allExpressions: _*)
        frame.show

        frame
    }

    /**
     * 统一的数据转换接口
     * 将所有预处理步骤组合在一起
     *
     * @param batchDF 输入的批处理DataFrame
     * @param dbName 目标数据库名
     * @param tableName 目标表名
     * @param fieldTypeMappings 字段名到数据类型的映射列表
     * @return 完全处理后的DataFrame，可直接写入Hudi
     */
    def transform(batchDF: DataFrame,
                  dbName: String,
                  tableName: String,
                  fieldTypeMappings: Seq[(String, DataType)],
                  partitionExpr: String): DataFrame = {

        logger.info(s"开始转换数据 - 数据库: $dbName, 表: $tableName")

        // 1. 根据事件类型处理数据字段
        val processedDF = processDataFieldsByEventType(batchDF)
        logger.info("步骤1: 事件类型数据字段处理完成")

        // 2. 过滤指定数据库和表名的数据
        val filteredDF = filterByDatabaseAndTable(processedDF, dbName, tableName)
        filteredDF.show
        val filteredCount = filteredDF.count()
        logger.info(s"步骤2: 数据库和表名过滤完成，过滤后数据量: $filteredCount 条")

        if (filteredCount > 0) {

            // 3. 提取字段并进行类型转换
            val convertedDF = extractAndConvertFields(filteredDF, fieldTypeMappings, partitionExpr)
            logger.info("步骤3: 字段提取和类型转换完成")

            // 检查转换结果
            val finalCount = convertedDF.count()
            logger.info(s"数据转换完成 - 最终数据量: $finalCount 条")

            if (finalCount > 0) {
                logger.info("转换后的数据样例:")
                convertedDF.show(3, truncate = false)
            } else {
                logger.info("警告: 字段提取后数据为空，可能存在字段匹配问题")
            }

            convertedDF
        } else {
            logger.info("没有匹配的数据需要转换")
            // 返回空的DataFrame，仅包含指定字段和附加字段的schema结构
            val emptySchema = StructType(
                fieldTypeMappings.map { case (fieldName, dataType) => StructField(fieldName, dataType, nullable = true) } ++
                    Seq(
                        StructField("cdc_dt", StringType, nullable = true),
                        StructField("cdc_delete_flag", IntegerType, nullable = true)
                    )
            )
            spark.createDataFrame(spark.sparkContext.emptyRDD[org.apache.spark.sql.Row], emptySchema)
        }
    }

    /**
     * 批量转换多个表的数据
     *
     * @param batchDF 输入的批处理DataFrame
     * @param tableConfigs 表配置列表，格式：Seq((数据库名, 表名, 字段类型映射))
     * @return 转换后的DataFrame列表
     */
    def batchTransform(batchDF: DataFrame,
                       tableConfigs: Seq[TableConfig]): Seq[DataFrame] = {
        tableConfigs.map { case tableConfig: TableConfig =>
            transform(batchDF
            , tableConfig.sourceDb.getOrElse("")
            , tableConfig.sourceTable.getOrElse("")
            , tableConfig.fieldMapping
            , tableConfig.partitionExpr.getOrElse("trunc(create_time, 'year')"))
        }.filter(_.count() > 0) // 过滤掉空的DataFrame
    }
}

/**
 * TdsqlBatchProcessor伴生对象
 */
object TdsqlBatchProcessor {

    /**
     * 创建TdsqlBatchProcessor实例
     *
     * @param spark SparkSession实例
     * @return TdsqlBatchProcessor实例
     */
    def apply(spark: SparkSession): TdsqlBatchProcessor = {
        new TdsqlBatchProcessor(spark)
    }
}
