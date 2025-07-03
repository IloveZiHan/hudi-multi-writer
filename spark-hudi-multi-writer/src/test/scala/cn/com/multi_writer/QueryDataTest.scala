package cn.com.multi_writer

import org.apache.spark.sql.SparkSession

object QueryDataTest {
    def main(args: Array[String]): Unit = {
        val spark = SparkSessionManager.createSparkSession("test")

        val hudiTablePath = "/tmp/spark-warehouse/s009_t_alc_loan"
        val hudiTable = spark.read.format("hudi").load(hudiTablePath)
        hudiTable.show(false)
    }
}   