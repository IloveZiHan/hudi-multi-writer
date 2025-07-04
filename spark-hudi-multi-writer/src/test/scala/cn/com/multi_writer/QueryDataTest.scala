package cn.com.multi_writer

import org.apache.spark.sql.SparkSession

object QueryDataTest {
    def main(args: Array[String]): Unit = {
        val spark = SparkSessionManager.createSparkSession("test")

        val hudiTablePath1 = "/tmp/spark-warehouse/s009_t_alc_loan"
        val hudiTable1 = spark.read.format("hudi").load(hudiTablePath1)

        // 查看hudi表的schema
        val hudiTable1Schema = hudiTable1.schema
        // 只打印schea的字段列表
        hudiTable1Schema.foreach(field => println(field.name))

        println("--------------------------------")

        val hudiTablePath2 = "/tmp/spark-warehouse/s009_t_alc_batch_repay"
        val hudiTable2 = spark.read.format("hudi").load(hudiTablePath2)

        val hudiTable2Schema = hudiTable2.schema
        hudiTable2Schema.foreach(field => println(field.name))

        // 打印两个表的数据
        hudiTable1.show(false)
        
        println("--------------------------------")
        hudiTable2.show(false)
    }
}   