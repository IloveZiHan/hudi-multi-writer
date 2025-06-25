package cn.com.multi_writer

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

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
  import spark.implicits._
  
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
  def extractAndConvertFields(batchDF: DataFrame, fieldTypeMappings: Seq[(String, DataType)]): DataFrame = {
    // 创建UDF函数来根据索引提取字段值
    val extractFieldByIndexUDF = udf((dataFields: Seq[String], columnNames: Seq[String], targetFieldName: String) => {
      if (dataFields != null && columnNames != null && targetFieldName != null) {
        // 找到目标字段在列名数组中的索引
        val index = columnNames.indexOf(targetFieldName)
        if (index >= 0 && index < dataFields.length) {
          dataFields(index)
        } else {
          null
        }
      } else {
        null
      }
    })
    
    // 创建一个临时DataFrame用于字段提取和转换
    var tempDF = batchDF
    
    // 为每个字段添加提取和转换逻辑
    fieldTypeMappings.foreach { case (fieldName, dataType) =>
      val extractedColName = s"extracted_$fieldName"
      
      // 先提取字段值
      tempDF = tempDF.withColumn(extractedColName, 
        extractFieldByIndexUDF(col("data_fields"), col("column"), lit(fieldName)))
      
      // 根据数据类型进行转换
      val convertedColName = fieldName
      tempDF = dataType match {
        case StringType =>
          tempDF.withColumn(convertedColName, col(extractedColName).cast(StringType))
        case IntegerType =>
          tempDF.withColumn(convertedColName, 
            when(col(extractedColName).isNull || col(extractedColName) === "", lit(null))
              .otherwise(col(extractedColName).cast(IntegerType)))
        case LongType =>
          tempDF.withColumn(convertedColName,
            when(col(extractedColName).isNull || col(extractedColName) === "", lit(null))
              .otherwise(col(extractedColName).cast(LongType)))
        case DoubleType =>
          tempDF.withColumn(convertedColName,
            when(col(extractedColName).isNull || col(extractedColName) === "", lit(null))
              .otherwise(col(extractedColName).cast(DoubleType)))
        case BooleanType =>
          tempDF.withColumn(convertedColName,
            when(col(extractedColName).isNull || col(extractedColName) === "", lit(null))
              .when(col(extractedColName) === "1" || col(extractedColName) === "true", lit(true))
              .when(col(extractedColName) === "0" || col(extractedColName) === "false", lit(false))
              .otherwise(lit(null)))
        case TimestampType =>
          tempDF.withColumn(convertedColName,
            when(col(extractedColName).isNull || col(extractedColName) === "", lit(null))
              .otherwise(to_timestamp(col(extractedColName))))
        case DateType =>
          tempDF.withColumn(convertedColName,
            when(col(extractedColName).isNull || col(extractedColName) === "", lit(null))
              .otherwise(to_date(col(extractedColName))))
        case _ =>
          // 默认情况下保持字符串类型
          tempDF.withColumn(convertedColName, col(extractedColName))
      }
      
      // 删除临时的提取列
      tempDF = tempDF.drop(extractedColName)
    }
    
    // 添加CDC相关的附加字段
    val dfWithCdcFields = tempDF
      // 添加cdc_dt字段：基于create_time获取那一年的第一天
      .withColumn("cdc_dt", 
        when(col("create_time").isNotNull, 
          date_format(
            date_trunc("year", to_timestamp(col("create_time"))),
            "yyyy-MM-dd"
          )
        ).otherwise(date_format(date_trunc("year", current_date()), "yyyy-MM-dd"))
      )
      // 添加cdc_delete_flag字段：delete事件为1，其他为0
      .withColumn("cdc_delete_flag",
        when(lower(col("eventtypestr")) === "delete", lit(1))
          .otherwise(lit(0))
      )
    
    // 仅选择fieldTypeMappings中定义的字段和附加字段
    val selectColumns = fieldTypeMappings.map(_._1) ++ Seq("cdc_dt", "cdc_delete_flag")
    
    dfWithCdcFields.select(selectColumns.map(col): _*)
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
                fieldTypeMappings: Seq[(String, DataType)]): DataFrame = {
    
    println(s"开始转换数据 - 数据库: $dbName, 表: $tableName")
    
    // 1. 根据事件类型处理数据字段
    val processedDF = processDataFieldsByEventType(batchDF)
    println("步骤1: 事件类型数据字段处理完成")
    
    // 2. 过滤指定数据库和表名的数据
    val filteredDF = filterByDatabaseAndTable(processedDF, dbName, tableName)
    val filteredCount = filteredDF.count()
    println(s"步骤2: 数据库和表名过滤完成，过滤后数据量: $filteredCount 条")
    
    if (filteredCount > 0) {
      // 3. 提取字段并进行类型转换
      val convertedDF = extractAndConvertFields(filteredDF, fieldTypeMappings)
      println("步骤3: 字段提取和类型转换完成")
      
      // 由于extractAndConvertFields已经仅返回指定字段，这里直接返回结果
      println(s"数据转换完成 - 最终数据量: ${convertedDF.count()} 条")
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
                    tableConfigs: Seq[(String, String, Seq[(String, DataType)])]): Seq[DataFrame] = {
    tableConfigs.map { case (dbName, tableName, fieldTypeMappings) =>
      transform(batchDF, dbName, tableName, fieldTypeMappings)
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
