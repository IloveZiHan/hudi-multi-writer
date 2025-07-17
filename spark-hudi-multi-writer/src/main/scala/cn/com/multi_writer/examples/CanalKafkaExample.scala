package cn.com.multi_writer.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import cn.com.multi_writer.source.CanalKafkaSource
import cn.com.multi_writer.schema.CanalKafkaSchema

/**
 * Canal Kafka数据源使用示例
 * 展示如何使用优化后的Schema方式解析Canal JSON数据
 */
object CanalKafkaExample {

  def main(args: Array[String]): Unit = {
    // 创建SparkSession
    val spark = SparkSession.builder()
      .appName("CanalKafkaExample")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try {
      // 设置Kafka配置
      spark.conf.set("spark.kafka.bootstrap.servers", "localhost:9092")
      spark.conf.set("spark.kafka.topic", "canal_topic")
      spark.conf.set("spark.kafka.starting.offsets", "earliest")

      // 创建Canal Kafka数据源
      val canalSource = new CanalKafkaSource(spark)

      // 示例1：基本的Canal数据解析
      println("=== 示例1：基本Canal数据解析 ===")
      basicCanalParsing(canalSource)

      // 示例2：扩展的Canal数据解析
      println("\n=== 示例2：扩展Canal数据解析 ===")
      extendedCanalParsing(canalSource)

      // 示例3：特定表的数据流
      println("\n=== 示例3：特定表的数据流 ===")
      specificTableStream(canalSource)

      // 示例4：数据类型验证
      println("\n=== 示例4：数据类型验证 ===")
      dataTypeValidation(spark)

      // 示例5：Canal数据展开处理
      println("\n=== 示例5：Canal数据展开处理 ===")
      dataExpansionExample(spark)

    } finally {
      spark.stop()
    }
  }

  /**
   * 基本的Canal数据解析示例
   */
  def basicCanalParsing(canalSource: CanalKafkaSource): Unit = {
    val parsedStream = canalSource.createParsedStream()
    
    println("基本解析后的Schema:")
    parsedStream.printSchema()
    
    // 显示解析后的数据样例
    val query = parsedStream.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", false)
      .option("numRows", 5)
      .start()
    
    Thread.sleep(5000) // 等待5秒观察数据
    query.stop()
  }

  /**
   * 扩展的Canal数据解析示例
   */
  def extendedCanalParsing(canalSource: CanalKafkaSource): Unit = {
    val extendedStream = canalSource.createExtendedParsedStream()
    
    println("扩展解析后的Schema:")
    extendedStream.printSchema()
    
    // 过滤非空数据并显示统计信息
    val statsStream = extendedStream
      .filter(col("has_data") === true)
      .select(
        col("database"),
        col("table"),
        col("event_type"),
        col("operation"),
        col("data_count"),
        col("pk_count"),
        col("mysql_type_count"),
        col("partition_key"),
        col("process_time")
      )
    
    val query = statsStream.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", false)
      .start()
    
    Thread.sleep(5000)
    query.stop()
  }

  /**
   * 特定表的数据流示例
   */
  def specificTableStream(canalSource: CanalKafkaSource): Unit = {
    // 获取特定数据库和表的数据流
    val tableStream = canalSource.getTableStream("sms", "mcs_sms_record_week")
    
    // 处理特定表的数据
    val processedStream = tableStream
      .filter(col("event_type").isin("INSERT", "UPDATE"))
      .select(
        col("database"),
        col("table"),
        col("event_type"),
        col("data_count"),
        col("binlog_timestamp"),
        col("process_time")
      )
    
    val query = processedStream.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", false)
      .start()
    
    Thread.sleep(5000)
    query.stop()
  }

  /**
   * 数据类型验证示例
   */
  def dataTypeValidation(spark: SparkSession): Unit = {
    import spark.implicits._
    
    // 模拟Canal JSON数据
    val testJsonData = Seq(
      """{
        "data": [{"id": "1001", "name": "测试1", "age": "25"}],
        "database": "test_db",
        "table": "test_table",
        "type": "INSERT",
        "ts": 1652720193306,
        "es": 1652720193000,
        "isDdl": false,
        "pkNames": ["id"],
        "mysqlType": {"id": "bigint", "name": "varchar(50)", "age": "int"},
        "sqlType": {"id": -5, "name": 12, "age": 4},
        "old": null
      }"""
    ).toDF("message_value")
    
    // 使用Canal schema解析
    val parsedDF = testJsonData
      .withColumn("canal_data", from_json(col("message_value"), CanalKafkaSchema.canalBinlogSchema))
      .select(
        col("canal_data.database").alias("database"),
        col("canal_data.table").alias("table"),
        col("canal_data.type").alias("event_type"),
        col("canal_data.ts").alias("binlog_timestamp"),
        col("canal_data.isDdl").alias("is_ddl"),
        col("canal_data.pkNames").alias("pk_names"),
        col("canal_data.mysqlType").alias("mysql_type"),
        col("canal_data.sqlType").alias("sql_type"),
        col("canal_data.data").alias("data"),
        col("canal_data.old").alias("old")
      )
    
    println("解析后的数据类型验证:")
    parsedDF.printSchema()
    parsedDF.show(false)
    
    // 验证具体字段类型
    val result = parsedDF.collect()(0)
    println(s"数据库: ${result.getAs[String]("database")}")
    println(s"表名: ${result.getAs[String]("table")}")
    println(s"时间戳: ${result.getAs[Long]("binlog_timestamp")}")
    println(s"主键字段: ${result.getAs[Seq[String]]("pk_names").mkString(", ")}")
    println(s"MySQL类型: ${result.getAs[Map[String, String]]("mysql_type")}")
    println(s"SQL类型: ${result.getAs[Map[String, Int]]("sql_type")}")
  }

  /**
   * Canal数据展开处理示例
   */
  def dataExpansionExample(spark: SparkSession): Unit = {
    import spark.implicits._
    
    // 模拟包含多条记录的Canal数据
    val multiRecordData = Seq(
      (
        "test_db",
        "user_table",
        "INSERT",
        Seq(
          Map("user_id" -> "1001", "name" -> "张三", "age" -> "25"),
          Map("user_id" -> "1002", "name" -> "李四", "age" -> "30"),
          Map("user_id" -> "1003", "name" -> "王五", "age" -> "28")
        ),
        Seq.empty[Map[String, String]]
      )
    ).toDF("database", "table", "event_type", "data", "old")
    
    // 展开数据数组
    val expandedDF = multiRecordData
      .select(
        col("database"),
        col("table"),
        col("event_type"),
        posexplode(col("data")).as(Seq("pos", "parsed_data"))
      )
      .withColumn("user_id", col("parsed_data").getItem("user_id"))
      .withColumn("name", col("parsed_data").getItem("name"))
      .withColumn("age", col("parsed_data").getItem("age").cast(IntegerType))
    
    println("Canal数据展开结果:")
    expandedDF.show(false)
    
    // 进一步处理展开后的数据
    val processedDF = expandedDF
      .filter(col("age") >= 26)
      .select(
        col("database"),
        col("table"),
        col("user_id"),
        col("name"),
        col("age"),
        current_timestamp().alias("process_time")
      )
    
    println("处理后的数据:")
    processedDF.show(false)
  }
} 