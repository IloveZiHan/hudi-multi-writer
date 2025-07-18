package cn.com.multi_writer.schema

import org.apache.spark.sql.types._

object BuriedpointKafkaSchema {

    val buriedpointKafkaSchema = MapType(StringType, StringType)
}