package cn.com.multi_writer.etl

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 数据清洗器类
 * 负责处理从Kafka消费到的数据，进行格式清洗和转换
 */
class DataCleaner(spark: SparkSession) {

    // 导入隐式转换

    /**
     * 清洗数据：处理column、where、field数组字段
     *
     * @param df 原始DataFrame
     * @return 清洗后的DataFrame
     */
    def cleanData(df: DataFrame): DataFrame = {
        // 创建UDF函数来处理column数组：移除前后的反引号
        val cleanColumnUDF = udf((arr: Seq[String]) => {
            if (arr != null) {
                arr.map { str =>
                    if (str != null && str.startsWith("`") && str.endsWith("`") && str.length > 1) {
                        str.substring(1, str.length - 1) // 移除前后的反引号
                    } else {
                        str
                    }
                }
            } else {
                arr
            }
        })

        // 创建UDF函数来处理where和field数组：移除首尾单引号，将##isnull##转换为null
        val cleanWhereFieldUDF = udf((arr: Seq[String]) => {
            if (arr != null) {
                arr.map { str =>
                    if (str != null) {
                        // 移除首尾的单引号
                        val withoutQuotes = if (str.startsWith("'") && str.endsWith("'") && str.length > 1) {
                            str.substring(1, str.length - 1) // 移除首尾的单引号
                        } else {
                            str
                        }
                        // 将##isnull##转换为null
                        if (withoutQuotes == "##isnull##") null else withoutQuotes
                    } else {
                        str
                    }
                }
            } else {
                arr
            }
        })

        // 应用数据清洗转换
        df.withColumn("column", cleanColumnUDF(col("column")))
            .withColumn("where", cleanWhereFieldUDF(col("where")))
            .withColumn("field", cleanWhereFieldUDF(col("field")))
    }
} 