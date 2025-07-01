package cn.com.multi_writer.sink

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Hudi数据写入器
 * 用于将DataFrame数据写入到Hudi表中
 */
class HudiWriter(spark: SparkSession) {

    /**
     * 将DataFrame写入到Hudi表
     * @param batchDF 需要写入的DataFrame
     * @param hudiTableName Hudi表名
     * @param hudiTablePath Hudi表存储路径
     */
    def writeToHudi(batchDF: DataFrame, hudiTableName: String, hudiTablePath: String): Unit = {
      batchDF
        .write
        .format("hudi")
        .option("hoodie.table.name", hudiTableName)
        .mode("append")
        .save(hudiTablePath)
    }
}

/**
 * HudiWriter伴生对象
 */
object HudiWriter {

    /**
     * 创建HudiWriter实例
     * @param spark SparkSession实例
     * @return HudiWriter实例
     */
    def apply(spark: SparkSession): HudiWriter = {
        new HudiWriter(spark)
    }
}
