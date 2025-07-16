package cn.com.multi_writer.etl

import cn.com.multi_writer.meta.TableConfig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

/**
 * OGG微批处理预处理器
 * 用于将从Kafka消费的OGG binlog DataFrame预处理为待写入Hudi表的DataFrame
 *
 * 功能包括：
 * 1. 解析OGG操作类型（I/U/D对应insert/update/delete）
 * 2. 从表名中提取schema和table信息
 * 3. 从after字段中提取数据字段
 * 4. 获取待写入Hudi的字段并进行类型转换
 */
class OggBatchProcessor(spark: SparkSession) {

    private val logger: Logger = LoggerFactory.getLogger(classOf[OggBatchProcessor])

    /**
     * 标准化OGG操作类型
     * 将OGG的操作类型（I/U/D）转换为标准格式
     *
     * @param batchDF 输入的批处理DataFrame
     * @return 处理后的DataFrame，包含标准化的事件类型
     */
    def standardizeEventType(batchDF: DataFrame): DataFrame = {
        // 创建UDF函数来转换OGG操作类型
        val convertOpTypeUDF = udf((opType: String) => {
            if (opType != null) {
                opType.toUpperCase match {
                    case "I" => "INSERT"
                    case "U" => "UPDATE"
                    case "D" => "DELETE"
                    case _ => opType.toUpperCase
                }
            } else {
                null
            }
        })

        batchDF.withColumn("eventtypestr", convertOpTypeUDF(col("op_type")))
    }

    /**
     * 解析表名，从"SCHEMA.TABLE"格式中提取schema和table信息
     *
     * @param batchDF 输入的DataFrame
     * @return 添加了schema和table_name字段的DataFrame
     */
    def parseTableName(batchDF: DataFrame): DataFrame = {
        // 使用split函数分离schema和table名
        val splitTableUDF = udf((tableName: String) => {
            if (tableName != null && tableName.contains(".")) {
                val parts = tableName.split("\\.", 2)
                if (parts.length == 2) {
                    (parts(0), parts(1)) // (schema, table)
                } else {
                    (null, tableName)
                }
            } else {
                (null, tableName)
            }
        })

        batchDF
            .withColumn("table_parts", splitTableUDF(col("table")))
            .withColumn("db", col("table_parts._1"))
            .withColumn("table_name", col("table_parts._2"))
            .drop("table_parts")
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
            .filter(col("db") === dbName.toUpperCase)
            .filter(col("table_name") === tableName.toUpperCase)
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
        val cdcDeleteFlagExpression = when(upper(col("eventtypestr")) === "DELETE", lit(1))
            .otherwise(lit(0))
            .alias("cdc_delete_flag")
        
        Seq(cdcDtExpression, cdcDeleteFlagExpression)
    }

    /**
     * 从after字段中提取指定字段并进行类型转换
     *
     * @param batchDF 输入的DataFrame
     * @param fieldTypeMappings 字段名到数据类型的映射列表
     * @param partitionExpr 分区表达式
     * @return 转换后的DataFrame，包含指定字段和附加字段
     */
    def extractAndConvertFields(batchDF: DataFrame,
                                fieldTypeMappings: Seq[(String, DataType)],
                                partitionExpr: String): DataFrame = {

        // 从after字段中提取指定字段
        val fieldExpressions = fieldTypeMappings.map { case (fieldName, dataType) =>
            val oggFieldName = fieldName.toUpperCase
            // 处理空值情况，如果after字段为null或者不包含指定字段，则返回null
            val extractExpr = when(col("after").isNotNull && col("after").getField(oggFieldName).isNotNull,
                col("after").getField(oggFieldName).cast(dataType))
                .otherwise(lit(null).cast(dataType))
                .alias(fieldName)
            
            extractExpr
        }

        // 构建CDC字段表达式
        val cdcFieldExpressions = buildCdcFields(partitionExpr)

        // 组合所有字段表达式
        val allExpressions = fieldExpressions ++ cdcFieldExpressions

        val resultDF = batchDF.select(allExpressions: _*)
        
        // 调试输出
        logger.info("OGG字段提取和转换完成")
        if (logger.isDebugEnabled) {
            resultDF.show(5, truncate = false)
        }

        resultDF
    }

    /**
     * 统一的数据转换接口
     * 将所有预处理步骤组合在一起
     *
     * @param batchDF 输入的批处理DataFrame
     * @param dbName 目标数据库名
     * @param tableName 目标表名
     * @param fieldTypeMappings 字段名到数据类型的映射列表
     * @param partitionExpr 分区表达式
     * @return 完全处理后的DataFrame，可直接写入Hudi
     */
    def transform(batchDF: DataFrame,
                  dbName: String,
                  tableName: String,
                  fieldTypeMappings: Seq[(String, DataType)],
                  partitionExpr: String): DataFrame = {

        logger.info(s"开始转换OGG数据 - 数据库: $dbName, 表: $tableName")

        // 1. 标准化事件类型
        val standardizedDF = standardizeEventType(batchDF)
        logger.info("步骤1: OGG事件类型标准化完成")

        // 2. 解析表名
        val parsedDF = parseTableName(standardizedDF)
        logger.info("步骤2: 表名解析完成")

        // 3. 过滤指定数据库和表名的数据
        val filteredDF = filterByDatabaseAndTable(parsedDF, dbName, tableName)
        val filteredCount = filteredDF.count()
        logger.info(s"步骤3: 数据库和表名过滤完成，过滤后数据量: $filteredCount 条")

        if (filteredCount > 0) {
            // 4. 提取字段并进行类型转换
            val convertedDF = extractAndConvertFields(filteredDF, fieldTypeMappings, partitionExpr)
            logger.info("步骤4: 字段提取和类型转换完成")

            // 检查转换结果
            val finalCount = convertedDF.count()
            logger.info(s"OGG数据转换完成 - 最终数据量: $finalCount 条")

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
     * @param tableConfigs 表配置列表
     * @return 转换后的DataFrame列表
     */
    def batchTransform(batchDF: DataFrame,
                       tableConfigs: Seq[TableConfig]): Seq[DataFrame] = {
        tableConfigs.map { tableConfig =>
            transform(batchDF,
                tableConfig.sourceDb.getOrElse(""),
                tableConfig.sourceTable.getOrElse(""),
                tableConfig.fieldMapping,
                tableConfig.partitionExpr.getOrElse("trunc(CREATE_TIME, 'year')"))
        }.filter(_.count() > 0) // 过滤掉空的DataFrame
    }
}

/**
 * OggBatchProcessor伴生对象
 */
object OggBatchProcessor {

    /**
     * 创建OggBatchProcessor实例
     *
     * @param spark SparkSession实例
     * @return OggBatchProcessor实例
     */
    def apply(spark: SparkSession): OggBatchProcessor = {
        new OggBatchProcessor(spark)
    }
} 