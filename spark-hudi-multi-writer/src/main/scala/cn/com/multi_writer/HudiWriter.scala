package cn.com.multi_writer

import cn.com.multi_writer.util.SparkJobStatusManager
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
    
    // 1. 将batchDF注册为临时视图
    val tempViewName = s"tmp_batch_${hudiTableName}"
    batchDF.createOrReplaceTempView(tempViewName)
    
    // 2. 使用using语句创建Spark Hudi外表
    val createTableSql = 
      s"""
         |CREATE TABLE IF NOT EXISTS ${hudiTableName} 
         |USING HUDI
         |LOCATION '${hudiTablePath}'
         |""".stripMargin
    
    spark.sql(createTableSql)
    
    // 3. 执行SET语句，设置Hudi当前写入方式为upsert
    spark.sql("SET hoodie.sql.bulk.insert.enable=false")
    spark.sql("SET hoodie.sql.insert.mode=upsert")
    
    // 4. 拼接insert into语句，将临时视图数据写入到Hudi表
    val insertSql = s"INSERT INTO ${hudiTableName} SELECT * FROM ${tempViewName}"
    spark.sql(insertSql)
    
    println(s"Successfully wrote data to Hudi table: ${hudiTableName}")
  }
  
  /**
   * 将DataFrame写入到Hudi表（带有额外的Hudi配置）
   * @param batchDF 需要写入的DataFrame
   * @param hudiTableName Hudi表名
   * @param hudiTablePath Hudi表存储路径
   * @param recordKey 记录主键字段
   * @param precombineField 预合并字段
   * @param partitionPath 分区字段（可选）
   */
  def writeToHudiWithConfig(batchDF: DataFrame, 
                           hudiTableName: String, 
                           hudiTablePath: String,
                           recordKey: String,
                           precombineField: String,
                           partitionPath: String = ""): Unit = {
    
    // 1. 将batchDF注册为临时视图
    val tempViewName = s"tmp_batch_${hudiTableName}"
    batchDF.createOrReplaceTempView(tempViewName)
    
    // 2. 设置Hudi相关配置
    spark.sql("SET hoodie.sql.bulk.insert.enable=false").collect()
    spark.sql("SET hoodie.sql.insert.mode=upsert").collect()
    
    // 3. 创建Hudi表（如果不存在）
    val partitionClause = if (partitionPath.nonEmpty) s"PARTITIONED BY (${partitionPath})" else ""
    val createTableSql = 
      s"""
         |CREATE TABLE IF NOT EXISTS ${hudiTableName} 
         |USING HUDI
         |${partitionClause}
         |LOCATION '${hudiTablePath}'
         |""".stripMargin
    
    spark.sql(createTableSql)
    
    // 4. 执行插入操作
    val insertSql = s"INSERT INTO ${hudiTableName} SELECT * FROM ${tempViewName}"
    spark.sql(insertSql).collect()
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
