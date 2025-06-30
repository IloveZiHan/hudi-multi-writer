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
        .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
        .option("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.SimpleKeyGenerator")
        .option("hoodie.table.recordkey.fields", "id")
        .option("hoodie.datasource.write.operation", "upsert")
        .option("hoodie.datasource.insert.dup.policy", "insert")
        .option("hoodie.datasource.write.precombine.field", "update_time")
        .option("hoodie.datasource.write.partitionpath.field", "cdc_dt")
        .option("hoodie.clustering.inline", "true")
        .option("hoodie.index.type", "BUCKET")
        .option("hoodie.index.bucket.engine", "SIMPLE")
        .option("hoodie.bucket.index.num.buckets", "3")
        .option("hoodie.cleaner.commits.retained", "24")
        .option("hoodie.insert.shuffle.parallelism", "10")
        .option("hoodie.datasource.write.set.null.for.missing.columns", "true")
        .option("hoodie.schema.on.read.enable", "true")
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
