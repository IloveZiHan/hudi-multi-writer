package cn.com.multi_writer.meta

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/**
 * Spark SQL DataFrame Schema JSON 示例类
 * 演示如何创建 DataFrame 并获取其 Schema 的 JSON 字符串
 */
class SparkSqlDataFrameSchemaJson {

}

object SparkSqlDataFrameSchemaJson {

  def main(args: Array[String]): Unit = {
    // 创建 SparkSession
    val spark = SparkSession.builder()
      .appName("SparkSqlDataFrameSchemaJson")
      .master("local[*]")
      .getOrCreate()

    // 导入隐式转换
    import spark.implicits._

    try {
      // 定义一个简单的 StructType Schema
      val schema = StructType(Array(
        StructField("id", LongType, nullable = false),
        StructField("name", StringType, nullable = true),
        StructField("age", IntegerType, nullable = true),
        StructField("salary", DecimalType(10, 2), nullable = true),
        StructField("is_active", BooleanType, nullable = true),
        StructField("create_time", TimestampType, nullable = true),
        StructField("tags", ArrayType(StringType), nullable = true),
        StructField("address", StructType(Array(
          StructField("street", StringType, nullable = true),
          StructField("city", StringType, nullable = true),
          StructField("country", StringType, nullable = true)
        )), nullable = true)
      ))

      // 创建示例数据
      val data = Seq(
        (1L, "张三", 25, BigDecimal("50000.00"), true, 
         java.sql.Timestamp.valueOf("2023-01-01 10:00:00"), 
         Array("开发者", "后端"), 
         ("北京路100号", "北京", "中国")),
        (2L, "李四", 30, BigDecimal("60000.00"), true, 
         java.sql.Timestamp.valueOf("2023-02-01 11:00:00"), 
         Array("架构师", "前端"), 
         ("上海路200号", "上海", "中国")),
        (3L, "王五", 28, BigDecimal("55000.00"), false, 
         java.sql.Timestamp.valueOf("2023-03-01 12:00:00"), 
         Array("测试", "QA"), 
         ("广州路300号", "广州", "中国"))
      )

      // 创建 DataFrame
      val df = spark.createDataFrame(data).toDF(
        "id", "name", "age", "salary", "is_active", "create_time", "tags", "address"
      )

      // 打印 DataFrame 内容
      println("=== DataFrame 内容 ===")
      df.show()

      // 打印 Schema 结构
      println("\n=== Schema 结构 ===")
      df.printSchema()

      // 获取并打印 Schema 的 JSON 字符串
      val schemaJson = df.schema.json
      println("\n=== Schema JSON 字符串 ===")
      println(schemaJson)

      // 格式化打印 Schema JSON（使用 FastJSON 格式化）
      try {
        import com.alibaba.fastjson.JSON
        val prettyJson = JSON.toJSONString(JSON.parseObject(schemaJson), true)
        println("\n=== 格式化的 Schema JSON ===")
        println(prettyJson)
      } catch {
        case e: Exception =>
          println(s"格式化 JSON 时出错: ${e.getMessage}")
      }

      // 演示如何从 JSON 字符串重新构建 Schema
      println("\n=== 从 JSON 重新构建 Schema ===")
      val rebuiltSchema = DataType.fromJson(schemaJson).asInstanceOf[StructType]
      println(s"重新构建的 Schema 字段数: ${rebuiltSchema.fields.length}")
      rebuiltSchema.fields.foreach { field =>
        println(s"  字段: ${field.name}, 类型: ${field.dataType}, 可空: ${field.nullable}")
      }

    } catch {
      case e: Exception =>
        println(s"执行过程中发生错误: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      // 关闭 SparkSession
      spark.stop()
    }
  }
}
