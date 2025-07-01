package cn.com.multi_writer.etl

import cn.com.multi_writer.meta.TableConfig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

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
        var cdcDtExpression: Column = null
        if(partitionExpr.isEmpty) {
            cdcDtExpression = expr("'default'").cast(StringType).alias("cdc_dt")
        } else {
            cdcDtExpression = expr(partitionExpr).cast(StringType).alias("cdc_dt")
        }

        val cdcDeleteFlagExpression = when(lower(col("eventtypestr")) === "delete", lit(1))
            .otherwise(lit(0))
            .alias("cdc_delete_flag")

        
        // 添加CDC字段
        val allExpressions = fieldExpressions ++ Seq(cdcDtExpression, cdcDeleteFlagExpression)

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

        println(s"开始转换数据 - 数据库: $dbName, 表: $tableName")

        // 1. 根据事件类型处理数据字段
        val processedDF = processDataFieldsByEventType(batchDF)
        println("步骤1: 事件类型数据字段处理完成")

        // 2. 过滤指定数据库和表名的数据
        val filteredDF = filterByDatabaseAndTable(processedDF, dbName, tableName)
        filteredDF.show
        val filteredCount = filteredDF.count()
        println(s"步骤2: 数据库和表名过滤完成，过滤后数据量: $filteredCount 条")

        if (filteredCount > 0) {

            // 3. 提取字段并进行类型转换
            val convertedDF = extractAndConvertFields(filteredDF, fieldTypeMappings, partitionExpr)
            println("步骤3: 字段提取和类型转换完成")

            // 检查转换结果
            val finalCount = convertedDF.count()
            println(s"数据转换完成 - 最终数据量: $finalCount 条")

            if (finalCount > 0) {
                println("转换后的数据样例:")
                convertedDF.show(3, truncate = false)
            } else {
                println("警告: 字段提取后数据为空，可能存在字段匹配问题")
            }

            convertedDF
        } else {
            println("没有匹配的数据需要转换")
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

    /**
     * 调试方法：检查数据结构和字段匹配情况
     *
     * @param batchDF 输入的DataFrame
     * @param targetFields 目标字段列表
     */
    def debugDataStructure(batchDF: DataFrame, targetFields: Seq[String] = Seq()): Unit = {
        println("=== 数据结构调试信息 ===")

        // 显示DataFrame的schema
        println("DataFrame Schema:")
        batchDF.printSchema()

        // 显示前几行数据
        println("前3行数据:")
        batchDF.show(3, truncate = false)

        // 检查特定字段的数据
        if (batchDF.columns.contains("data_fields") && batchDF.columns.contains("column")) {
            println("data_fields和column字段的详细信息:")
            batchDF.select("data_fields", "column").show(3, truncate = false)

            // 检查字段匹配情况
            if (targetFields.nonEmpty) {
                println(s"目标字段: ${targetFields.mkString(", ")}")

                // 创建调试UDF来检查字段匹配
                val debugUDF = udf((dataFields: Seq[String], columnNames: Seq[String]) => {
                    if (dataFields != null && columnNames != null) {
                        val info = Map(
                            "data_fields_count" -> dataFields.length,
                            "column_names_count" -> columnNames.length,
                            "column_names" -> columnNames.mkString("[", ", ", "]"),
                            "data_fields_sample" -> dataFields.take(5).mkString("[", ", ", "]")
                        )
                        info.toString
                    } else {
                        "data_fields or column_names is null"
                    }
                })

                val debugDF = batchDF.withColumn("debug_info",
                    debugUDF(col("data_fields"), col("column")))

                println("字段匹配调试信息:")
                debugDF.select("debug_info").show(3, truncate = false)
            }
        } else {
            println("缺少必要的字段: data_fields 或 column")
        }

        println("=== 调试信息结束 ===")
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

    /**
     * 常用数据类型映射工具方法
     *
     * @param fieldName 字段名
     * @param typeName 类型名称字符串（如："string", "int", "long", "double", "boolean", "timestamp", "date"）
     * @return 字段名和DataType的元组
     */
    def createFieldMapping(fieldName: String, typeName: String): (String, DataType) = {
        val dataType = typeName.toLowerCase match {
            case "string" | "varchar" | "text" => StringType
            case "int" | "integer" => IntegerType
            case "long" | "bigint" => LongType
            case "double" | "decimal" | "float" => DoubleType
            case "boolean" | "bool" => BooleanType
            case "timestamp" | "datetime" => TimestampType
            case "date" => DateType
            case _ => StringType // 默认为字符串类型
        }
        (fieldName, dataType)
    }

    /**
     * 批量创建字段映射
     *
     * @param fieldTypePairs 字段名和类型名的对列表
     * @return 字段映射列表
     */
    def createFieldMappings(fieldTypePairs: (String, String)*): Seq[(String, DataType)] = {
        fieldTypePairs.map { case (fieldName, typeName) =>
            createFieldMapping(fieldName, typeName)
        }
    }
}
