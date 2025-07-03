package cn.com.multi_writer.meta

import cn.com.multi_writer.meta.MetaHudiTableManager.TableStatus
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.config.HoodieCommonConfig
import org.apache.hudi.AvroConversionUtils
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.{Logger, LoggerFactory}

import java.net.URI
import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties
import scala.io.Source
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature

import scala.collection.JavaConverters._

/**
 * Hudi表元数据管理器
 *
 * 用于管理Hudi表的元数据信息，包括表结构schema、状态、标签等信息
 *
 * 表字段说明：
 * - id: 表的唯一标识符
 * - schema: 表结构的JSON字符串表示
 * - status: 表状态，0-未上线，1-已上线
 * - is_partitioned: 是否为分区表，true-分区表，false-非分区表
 * - partition_expr: 分区表达式，表示如何来计算分区值的
 * - hoodie_config: Hudi表配置的JSON串，用于保存创建表时Hudi表的配置
 * - tags: 表标签，多个标签用逗号分隔
 * - description: 表描述信息
 * - source_db: 源数据库名称
 * - source_table: 源表名称
 * - db_type: 数据库类型（如：MySQL、PostgreSQL、Oracle等）
 *
 * 使用示例：
 * val metaManager = new MetaHudiTableManager(spark)
 * metaManager.createMetaTable("/tmp/hudi_tables/meta_hudi_table")
 * metaManager.insertTableMeta("t_alc_loan", schemaJson, 1, "业务表,核心表", "贷款信息表", "finance_db", "loan_info", "MySQL")
 */
class MetaHudiTableManager(spark: SparkSession) {

    private val logger: Logger = LoggerFactory.getLogger(classOf[MetaHudiTableManager])
    
    import spark.implicits._

    /**
     * 创建元数据表的Spark SQL DDL
     *
     * @param tablePath 表存储路径
     * @return DDL字符串
     */
    def getCreateTableDDL(tablePath: String): String = {
        s"""
           |CREATE TABLE IF NOT EXISTS meta_hudi_table (
           |    id STRING NOT NULL COMMENT '表唯一标识符',
           |    schema STRING NOT NULL COMMENT '表结构JSON字符串',
           |    status INT NOT NULL COMMENT '表状态：0-未上线，1-已上线',
           |    is_partitioned BOOLEAN NOT NULL COMMENT '是否为分区表：true-分区表，false-非分区表',
           |    partition_expr STRING COMMENT '分区表达式，表示如何来计算分区值的',
           |    hoodie_config STRING COMMENT 'Hudi表配置的JSON串，用于保存创建表时Hudi表的配置',
           |    tags STRING COMMENT '表标签，多个标签用逗号分隔',
           |    description STRING COMMENT '表描述信息',
           |    source_db STRING COMMENT '源数据库名称',
           |    source_table STRING COMMENT '源表名称',
           |    db_type STRING COMMENT '数据库类型：MySQL、PostgreSQL、Oracle等',
           |    create_time TIMESTAMP NOT NULL COMMENT '创建时间',
           |    update_time TIMESTAMP NOT NULL COMMENT '更新时间'
           |) USING HUDI
           |LOCATION '$tablePath'
           |TBLPROPERTIES (
           |    'hoodie.table.name' = 'meta_hudi_table',
           |    'hoodie.datasource.write.recordkey.field' = 'id',
           |    'hoodie.datasource.write.precombine.field' = 'update_time',
           |    'hoodie.datasource.write.table.name' = 'meta_hudi_table',
           |    'hoodie.datasource.write.operation' = 'upsert',
           |    'hoodie.datasource.write.table.type' = 'COPY_ON_WRITE',
           |    'hoodie.datasource.write.hive_style_partitioning' = 'false',
           |    'hoodie.index.type' = 'BLOOM',
           |    'hoodie.bloom.index.bucketized.checking' = 'true',
           |    'hoodie.datasource.write.payload.class' = 'org.apache.hudi.common.model.PartialUpdateAvroPayload'
           |)
           |""".stripMargin
    }

    /**
     * 创建元数据表
     *
     * @param tablePath 表存储路径
     * @return 是否创建成功
     */
    def createMetaTable(tablePath: String): Boolean = {
        try {
            logger.info(s"开始创建Hudi元数据表: meta_hudi_table")
            logger.info(s"表路径: $tablePath")

            // 执行DDL创建表
            val ddl = getCreateTableDDL(tablePath)
            logger.info("执行DDL:")
            logger.info(ddl)

            spark.sql(ddl)

            logger.info("✓ Hudi元数据表创建成功，已启用partial update支持")
            true

        } catch {
            case e: Exception =>
                logger.error(s"✗ 创建Hudi元数据表失败: ${e.getMessage}", e)
                false
        }
    }

    /**
     * 插入或更新表元数据记录
     *
     * @param tableId     表标识符
     * @param schemaJson  表结构JSON字符串
     * @param status      表状态（0-未上线，1-已上线）
     * @param isPartitioned 是否为分区表
     * @param partitionExpr 分区表达式，表示如何来计算分区值的
     * @param hoodieConfig Hudi表配置的JSON串，用于保存创建表时Hudi表的配置
     * @param tags        表标签
     * @param description 表描述
     * @param sourceDb    源数据库名称
     * @param sourceTable 源表名称
     * @param dbType      数据库类型
     * @param tablePath   元数据表路径
     * @return 是否操作成功
     */
    def insertTableMeta(tableId: String,
                        schemaJson: String,
                        status: Int,
                        isPartitioned: Boolean,
                        partitionExpr: String = null,
                        hoodieConfig: String = null,
                        tags: String = null,
                        description: String = null,
                        sourceDb: String = null,
                        sourceTable: String = null,
                        dbType: String = null,
                        tablePath: String): Boolean = {
        try {
            val currentTime = new java.sql.Timestamp(System.currentTimeMillis())

            // 创建DataFrame
            val metaData = Seq(
                MetaHudiTableRecord(
                    id = tableId,
                    schema = schemaJson,
                    status = status,
                    is_partitioned = isPartitioned,
                    partition_expr = Option(partitionExpr),
                    hoodie_config = Option(hoodieConfig),
                    tags = Option(tags),
                    description = Option(description),
                    source_db = Option(sourceDb),
                    source_table = Option(sourceTable),
                    db_type = Option(dbType),
                    create_time = currentTime,
                    update_time = currentTime
                )
            ).toDF()

            logger.info(s"插入元数据记录: $tableId (分区表: $isPartitioned, 分区表达式: ${Option(partitionExpr).getOrElse("N/A")}, 源库: ${Option(sourceDb).getOrElse("N/A")}, 源表: ${Option(sourceTable).getOrElse("N/A")}, 数据库类型: ${Option(dbType).getOrElse("N/A")})")

            // 写入Hudi表
            metaData.write
                .format("hudi")
                .mode("append")
                .save(tablePath)

            logger.info(s"✓ 元数据记录插入成功: $tableId")
            true

        } catch {
            case e: Exception =>
                logger.error(s"✗ 插入元数据记录失败: ${e.getMessage}", e)
                false
        }
    }

    /**
     * 查询表元数据
     *
     * @param tablePath 元数据表路径
     * @return DataFrame包含所有元数据记录
     */
    def queryTableMeta(tablePath: String): DataFrame = {
        try {
            spark.read
                .format("hudi")
                .load(tablePath)
                .select("id", "schema", "status", "is_partitioned", "partition_expr", "hoodie_config", "tags", "description", 
                       "source_db", "source_table", "db_type", "create_time", "update_time")

        } catch {
            case e: Exception =>
                logger.error(s"查询元数据表失败: ${e.getMessage}", e)
                spark.emptyDataFrame
        }
    }

    /**
     * 根据表ID查询特定表的元数据
     *
     * @param tableId   表ID
     * @param tablePath 元数据表路径
     * @return DataFrame包含指定表的元数据记录
     */
    def queryTableMetaById(tableId: String, tablePath: String): DataFrame = {
        try {
            queryTableMeta(tablePath)
                .filter($"id" === tableId)

        } catch {
            case e: Exception =>
                logger.error(s"根据ID查询元数据失败: ${e.getMessage}", e)
                spark.emptyDataFrame
        }
    }

    /**
     * 更新表状态
     *
     * @param tableId   表ID
     * @param newStatus 新状态（0-未上线，1-已上线）
     * @param tablePath 元数据表路径
     * @return 是否更新成功
     */
    def updateTableStatus(tableId: String, newStatus: Int, tablePath: String): Boolean = {
        try {
            val currentTime = new java.sql.Timestamp(System.currentTimeMillis())

            // 先查询现有记录
            val existingRecord = queryTableMetaById(tableId, tablePath).collect()

            if (existingRecord.isEmpty) {
                logger.error(s"✗ 未找到表ID为 $tableId 的记录")
                return false
            }

            val record = existingRecord.head

            // 创建更新后的记录
            val updatedData = Seq(
                MetaHudiTableRecord(
                    id = tableId,
                    schema = record.getAs[String]("schema"),
                    status = newStatus,
                    is_partitioned = record.getAs[Boolean]("is_partitioned"),
                    partition_expr = Option(record.getAs[String]("partition_expr")),
                    hoodie_config = Option(record.getAs[String]("hoodie_config")),
                    tags = Option(record.getAs[String]("tags")),
                    description = Option(record.getAs[String]("description")),
                    source_db = Option(record.getAs[String]("source_db")),
                    source_table = Option(record.getAs[String]("source_table")),
                    db_type = Option(record.getAs[String]("db_type")),
                    create_time = record.getAs[java.sql.Timestamp]("create_time"),
                    update_time = currentTime
                )
            ).toDF()

            // 写入Hudi表进行更新（支持partial update）
            updatedData.write
                .format("hudi")
                .mode("append")
                .save(tablePath)

            logger.info(s"✓ 表状态更新成功: $tableId -> $newStatus")
            true

        } catch {
            case e: Exception =>
                logger.error(s"✗ 更新表状态失败: ${e.getMessage}", e)
                false
        }
    }

    /**
     * 根据状态查询表列表
     *
     * @param status    表状态（0-未上线，1-已上线）
     * @param tablePath 元数据表路径
     * @return DataFrame包含指定状态的表记录
     */
    def queryTablesByStatus(status: Int, tablePath: String): DataFrame = {
        try {
            queryTableMeta(tablePath)
                .filter($"status" === status)
                .orderBy($"update_time".desc)

        } catch {
            case e: Exception =>
                logger.error(s"根据状态查询表列表失败: ${e.getMessage}", e)
                spark.emptyDataFrame
        }
    }

    /**
     * 根据标签查询表列表
     *
     * @param tag       标签名称
     * @param tablePath 元数据表路径
     * @return DataFrame包含指定标签的表记录
     */
    def queryTablesByTag(tag: String, tablePath: String): DataFrame = {
        try {
            queryTableMeta(tablePath)
                .filter($"tags".contains(tag))
                .orderBy($"update_time".desc)

        } catch {
            case e: Exception =>
                logger.error(s"根据标签查询表列表失败: ${e.getMessage}", e)
                spark.emptyDataFrame
        }
    }

    /**
     * 根据是否分区查询表列表
     *
     * @param isPartitioned 是否为分区表
     * @param tablePath     元数据表路径
     * @return DataFrame包含指定分区类型的表记录
     */
    def queryTablesByPartitioned(isPartitioned: Boolean, tablePath: String): DataFrame = {
        try {
            queryTableMeta(tablePath)
                .filter($"is_partitioned" === isPartitioned)
                .orderBy($"update_time".desc)

        } catch {
            case e: Exception =>
                logger.error(s"根据分区类型查询表列表失败: ${e.getMessage}", e)
                spark.emptyDataFrame
        }
    }

    /**
     * 根据数据库类型查询表列表
     *
     * @param dbType    数据库类型（如：MySQL、PostgreSQL、Oracle等）
     * @param tablePath 元数据表路径
     * @return DataFrame包含指定数据库类型的表记录
     */
    def queryTablesByDbType(dbType: String, tablePath: String): DataFrame = {
        try {
            queryTableMeta(tablePath)
                .filter($"db_type" === dbType)
                .orderBy($"update_time".desc)

        } catch {
            case e: Exception =>
                logger.error(s"根据数据库类型查询表列表失败: ${e.getMessage}", e)
                spark.emptyDataFrame
        }
    }

    /**
     * 根据源数据库名称查询表列表
     *
     * @param sourceDb  源数据库名称
     * @param tablePath 元数据表路径
     * @return DataFrame包含指定源数据库的表记录
     */
    def queryTablesBySourceDb(sourceDb: String, tablePath: String): DataFrame = {
        try {
            queryTableMeta(tablePath)
                .filter($"source_db" === sourceDb)
                .orderBy($"update_time".desc)

        } catch {
            case e: Exception =>
                logger.error(s"根据源数据库查询表列表失败: ${e.getMessage}", e)
                spark.emptyDataFrame
        }
    }

    /**
     * 根据源表名称查询表列表（支持模糊匹配）
     *
     * @param sourceTable 源表名称
     * @param tablePath   元数据表路径
     * @param isExact     是否精确匹配，默认为true，false时进行模糊匹配
     * @return DataFrame包含匹配源表名称的表记录
     */
    def queryTablesBySourceTable(sourceTable: String, tablePath: String, isExact: Boolean = true): DataFrame = {
        try {
            val result = if (isExact) {
                queryTableMeta(tablePath)
                    .filter($"source_table" === sourceTable)
            } else {
                queryTableMeta(tablePath)
                    .filter($"source_table".contains(sourceTable))
            }
            
            result.orderBy($"update_time".desc)

        } catch {
            case e: Exception =>
                logger.error(s"根据源表名称查询表列表失败: ${e.getMessage}", e)
                spark.emptyDataFrame
        }
    }

    /**
     * 根据源数据库和源表名称查询特定表
     *
     * @param sourceDb    源数据库名称
     * @param sourceTable 源表名称
     * @param tablePath   元数据表路径
     * @return DataFrame包含匹配的表记录
     */
    def queryTableBySourceDbAndTable(sourceDb: String, sourceTable: String, tablePath: String): DataFrame = {
        try {
            queryTableMeta(tablePath)
                .filter($"source_db" === sourceDb && $"source_table" === sourceTable)
                .orderBy($"update_time".desc)

        } catch {
            case e: Exception =>
                logger.error(s"根据源数据库和源表查询失败: ${e.getMessage}", e)
                spark.emptyDataFrame
        }
    }

    /**
     * 更新表的上线/下线状态
     *
     * @param tableId   表ID
     * @param status    状态字符串："online"(上线)或"offline"(下线)
     * @param tablePath 元数据表路径
     * @return 是否更新成功
     */
    def updateTableOnlineStatus(tableId: String, status: String, tablePath: String): Boolean = {
        try {
            val statusValue = status.toLowerCase.trim match {
                case "1" => TableStatus.ONLINE
                case "0" => TableStatus.OFFLINE
                case _ =>
                    logger.error(s"✗ 无效的状态值: $status，支持的状态: online, offline")
                    return false
            }

            val statusText = if (statusValue == TableStatus.ONLINE) "上线" else "下线"
            logger.info(s"开始更新表 $tableId 状态为: $statusText")

            val result = updateTableStatus(tableId, statusValue, tablePath)
            
            if (result) {
                logger.info(s"✓ 表 $tableId 状态更新成功: $statusText")
            } else {
                logger.error(s"✗ 表 $tableId 状态更新失败: $statusText")
            }

            result

        } catch {
            case e: Exception =>
                logger.error(s"✗ 更新表状态失败: ${e.getMessage}", e)
                false
        }
    }

    /**
     * 设置表为上线状态
     *
     * @param tableId   表ID
     * @param tablePath 元数据表路径
     * @return 是否设置成功
     */
    def setTableOnline(tableId: String, tablePath: String): Boolean = {
        updateTableOnlineStatus(tableId, "online", tablePath)
    }

    /**
     * 设置表为下线状态
     *
     * @param tableId   表ID
     * @param tablePath 元数据表路径
     * @return 是否设置成功
     */
    def setTableOffline(tableId: String, tablePath: String): Boolean = {
        updateTableOnlineStatus(tableId, "offline", tablePath)
    }

    /**
     * 更新表的源信息（源数据库、源表名、数据库类型）
     *
     * @param tableId     表ID
     * @param sourceDb    源数据库名称
     * @param sourceTable 源表名称
     * @param dbType      数据库类型（如：MySQL、PostgreSQL、Oracle等）
     * @param tablePath   元数据表路径
     * @return 是否更新成功
     */
    def updateTableSourceInfo(tableId: String, 
                             sourceDb: String = null, 
                             sourceTable: String = null, 
                             dbType: String = null, 
                             tablePath: String): Boolean = {
        try {
            val currentTime = new java.sql.Timestamp(System.currentTimeMillis())

            // 先查询现有记录
            val existingRecord = queryTableMetaById(tableId, tablePath).collect()

            if (existingRecord.isEmpty) {
                logger.error(s"✗ 未找到表ID为 $tableId 的记录")
                return false
            }

            val record = existingRecord.head

            // 获取更新后的源信息（如果参数为null则保持原值）
            val updatedSourceDb = Option(sourceDb).getOrElse(record.getAs[String]("source_db"))
            val updatedSourceTable = Option(sourceTable).getOrElse(record.getAs[String]("source_table"))
            val updatedDbType = Option(dbType).getOrElse(record.getAs[String]("db_type"))

            logger.info(s"更新表 $tableId 的源信息:")
            logger.info(s"  源数据库: ${Option(record.getAs[String]("source_db")).getOrElse("N/A")} -> ${Option(updatedSourceDb).getOrElse("N/A")}")
            logger.info(s"  源表名: ${Option(record.getAs[String]("source_table")).getOrElse("N/A")} -> ${Option(updatedSourceTable).getOrElse("N/A")}")
            logger.info(s"  数据库类型: ${Option(record.getAs[String]("db_type")).getOrElse("N/A")} -> ${Option(updatedDbType).getOrElse("N/A")}")

            // 创建更新后的记录
            val updatedData = Seq(
                MetaHudiTableRecord(
                    id = tableId,
                    schema = record.getAs[String]("schema"),
                    status = record.getAs[Int]("status"),
                    is_partitioned = record.getAs[Boolean]("is_partitioned"),
                    partition_expr = Option(record.getAs[String]("partition_expr")),
                    hoodie_config = Option(record.getAs[String]("hoodie_config")),
                    tags = Option(record.getAs[String]("tags")),
                    description = Option(record.getAs[String]("description")),
                    source_db = Option(updatedSourceDb),
                    source_table = Option(updatedSourceTable),
                    db_type = Option(updatedDbType),
                    create_time = record.getAs[java.sql.Timestamp]("create_time"),
                    update_time = currentTime
                )
            ).toDF()

            // 写入Hudi表进行更新（支持partial update）
            updatedData.write
                .format("hudi")
                .mode("append")
                .save(tablePath)

            logger.info(s"✓ 表源信息更新成功: $tableId")
            true

        } catch {
            case e: Exception =>
                logger.error(s"✗ 更新表源信息失败: ${e.getMessage}", e)
                false
        }
    }

    /**
     * 设置表的源数据库信息
     *
     * @param tableId   表ID
     * @param sourceDb  源数据库名称
     * @param tablePath 元数据表路径
     * @return 是否设置成功
     */
    def setTableSourceDb(tableId: String, sourceDb: String, tablePath: String): Boolean = {
        updateTableSourceInfo(tableId, sourceDb = sourceDb, tablePath = tablePath)
    }

    /**
     * 设置表的源表名信息
     *
     * @param tableId     表ID
     * @param sourceTable 源表名称
     * @param tablePath   元数据表路径
     * @return 是否设置成功
     */
    def setTableSourceTable(tableId: String, sourceTable: String, tablePath: String): Boolean = {
        updateTableSourceInfo(tableId, sourceTable = sourceTable, tablePath = tablePath)
    }

    /**
     * 设置表的数据库类型信息
     *
     * @param tableId   表ID
     * @param dbType    数据库类型（如：MySQL、PostgreSQL、Oracle等）
     * @param tablePath 元数据表路径
     * @return 是否设置成功
     */
    def setTableDbType(tableId: String, dbType: String, tablePath: String): Boolean = {
        updateTableSourceInfo(tableId, dbType = dbType, tablePath = tablePath)
    }

    /**
     * 查询所有表
     *
     * @param tablePath 元数据表路径
     * @return DataFrame包含所有表记录
     */
    def queryAllTables(tablePath: String): DataFrame = {
        try {
            queryTableMeta(tablePath)
                .orderBy($"update_time".desc)
        } catch {
            case e: Exception =>
                logger.error(s"查询所有表失败: ${e.getMessage}", e)
                spark.emptyDataFrame
        }
    }

    /**
     * 查询所有已上线的Hudi表
     *
     * 该方法返回所有状态为"已上线"(status=1)的表记录，
     * 结果按更新时间倒序排列，便于查看最近上线的表
     *
     * @param tablePath 元数据表路径
     * @return DataFrame包含所有已上线表的记录
     */
    def queryOnlineTables(tablePath: String): DataFrame = {
        try {
            logger.info(s"开始查询所有已上线的Hudi表...")
            
            val onlineTables = queryTablesByStatus(TableStatus.ONLINE, tablePath)
            val count = onlineTables.count()
            
            logger.info(s"✓ 查询完成，共找到 $count 个已上线的表")
            
            onlineTables

        } catch {
            case e: Exception =>
                logger.error(s"✗ 查询已上线表失败: ${e.getMessage}", e)
                spark.emptyDataFrame
        }
    }

    /**
     * 从Hudi表路径提取schema并插入到元数据表中
     *
     * 该方法会：
     * 1. 读取指定路径的Hudi表
     * 2. 提取表的schema结构
     * 3. 将schema信息插入到元数据表中
     *
     * @param hudiTablePath Hudi表的存储路径
     * @param metaTablePath 元数据表的存储路径
     * @param status        表状态，默认为0（未上线）
     * @param tags          表标签，多个标签用逗号分隔
     * @param description   表描述信息
     * @param sourceDb      源数据库名称
     * @param sourceTable   源表名称
     * @param dbType        数据库类型
     * @return 是否操作成功
     */
    def extractAndInsertHudiTableSchema(hudiTablePath: String,
                                        metaTablePath: String,
                                        status: Int = 0,
                                        tags: String = null,
                                        description: String = null,
                                        sourceDb: String = null,
                                        sourceTable: String = null,
                                        dbType: String = null): Boolean = {
        try {
            logger.info(s"开始从Hudi表路径提取schema: $hudiTablePath")

            // 读取Hudi表获取schema
            val hudiTable = spark.read
                .format("hudi")
                .load(hudiTablePath)

            // 获取schema并转换为JSON字符串
            // 过滤掉Hudi自带的数据列和cdc_开头的数据列
            val filteredFields = hudiTable.schema.fields.filter { field =>
                val fieldName = field.name
                // 排除Hudi自带的元数据列
                !fieldName.startsWith("_hoodie_") &&
                // 排除cdc_开头的列
                !fieldName.startsWith("cdc_")
            }
            
            // 创建过滤后的schema
            val filteredSchema = StructType(filteredFields)
            val schemaJson = filteredSchema.json
            logger.info(s"提取到的schema（已过滤Hudi和CDC列）: ${schemaJson.take(200)}...")

            // 检测是否为分区表
            val isPartitioned = detectPartitionedTable(hudiTablePath)
            logger.info(s"检测到分区信息: ${if (isPartitioned) "分区表" else "非分区表"}")

            // 从路径中提取表名作为ID
            val tableId = extractTableNameFromPath(hudiTablePath)
            logger.info(s"提取的表ID: $tableId")

            // 插入元数据记录
            val success = insertTableMeta(
                tableId = tableId,
                schemaJson = schemaJson,
                status = status,
                isPartitioned = isPartitioned,
                partitionExpr = null, // 可以后续扩展，自动检测分区表达式
                hoodieConfig = null, // 可以后续扩展，自动读取Hudi配置
                tags = tags,
                description = description,
                sourceDb = sourceDb,
                sourceTable = sourceTable,
                dbType = dbType,
                tablePath = metaTablePath
            )

            if (success) {
                logger.info(s"✓ 成功从Hudi表提取schema并插入元数据表: $tableId (分区表: $isPartitioned)")
            } else {
                logger.error(s"✗ 从Hudi表提取schema并插入元数据表失败: $tableId")
            }

            success

        } catch {
            case e: Exception =>
                logger.error(s"✗ 从Hudi表提取schema失败: ${e.getMessage}", e)
                false
        }
    }

    /**
     * 检测Hudi表是否为分区表
     *
     * @param hudiTablePath Hudi表路径
     * @return 是否为分区表
     */
    private def detectPartitionedTable(hudiTablePath: String): Boolean = {
        try {
            // 读取Hudi表
            val hudiTable = spark.read
                .format("hudi")
                .load(hudiTablePath)

            // 尝试读取元数据来检测分区信息
            val catalog = spark.sessionState.catalog
            
            // 检查Hudi表属性中的分区配置
            val hudiBasedPartition = try {
                // 尝试从Hudi配置中获取分区信息
                val hudiDF = spark.read.format("hudi").load(hudiTablePath)
                val columns = hudiDF.columns.toSet
                // 检查是否包含Hudi分区相关的列（如以分区字段结尾的列）
                columns.exists(col => col.toLowerCase.contains("cdc_dt"))
            } catch {
                case _: Exception => false
            }

            val isPartitioned = hudiBasedPartition
            logger.info(s"分区检测结果 -  Hudi检测: $hudiBasedPartition, 最终结果: $isPartitioned")
            
            isPartitioned

        } catch {
            case e: Exception =>
                logger.error(s"检测分区表失败，默认为非分区表: ${e.getMessage}", e)
                false
        }
    }

    /**
     * 从Hudi表路径中提取表名
     *
     * @param hudiTablePath Hudi表路径
     * @return 表名
     */
    private def extractTableNameFromPath(hudiTablePath: String): String = {
        try {
            // 移除末尾的斜杠并获取最后一个路径段作为表名
            val cleanPath = hudiTablePath.stripSuffix("/")
            val pathSegments = cleanPath.split("/")
            val tableName = pathSegments.last

            // 如果表名为空，则使用倒数第二个路径段
            if (tableName.isEmpty && pathSegments.length > 1) {
                pathSegments(pathSegments.length - 2)
            } else {
                tableName
            }

        } catch {
            case e: Exception =>
                logger.error(s"从路径提取表名失败，使用默认名称: ${e.getMessage}", e)
                s"table_${System.currentTimeMillis()}"
        }
    }

    /**
     * 打印表的DDL语句，用于调试和文档目的
     */
    def printCreateTableDDL(tablePath: String): Unit = {
        logger.info("=== meta_hudi_table 创建DDL ===")
        logger.info(getCreateTableDDL(tablePath))
        logger.info("================================")
    }

    /**
     * 从Hudi表路径获取最新的表schema
     *
     * @param hudiTablePath Hudi表路径
     * @return 最新的表schema，如果获取失败则返回None
     */
    def getLatestHudiTableSchema(hudiTablePath: String): Option[StructType] = {
        try {
            logger.info(s"开始获取Hudi表最新schema: $hudiTablePath")
            
            // 检查路径是否存在
            val hadoopConf = spark.sparkContext.hadoopConfiguration
            val fs = FileSystem.get(new URI(hudiTablePath), hadoopConf)
            val path = new Path(hudiTablePath)
            
            if (!fs.exists(path)) {
                logger.error(s"✗ Hudi表路径不存在: $hudiTablePath")
                return None
            }

            // 创建HoodieTableMetaClient
            val metaClient = HoodieTableMetaClient.builder()
                .setConf(HadoopFSUtils.getStorageConfWithCopy(hadoopConf))
                .setBasePath(hudiTablePath)
                .build()

            // 使用TableSchemaResolver获取最新schema
            val schemaResolver = new TableSchemaResolver(metaClient)
            
            // 尝试从最新commit获取schema
            val latestAvroSchemaOpt = try {
                schemaResolver.getTableAvroSchemaFromLatestCommit(false)
            } catch {
                case e: Exception =>
                    logger.error(s"从最新commit获取schema失败，尝试其他方式: ${e.getMessage}", e)
                    try {
                        // 如果从commit获取失败，尝试从数据文件获取
                        val schema = schemaResolver.getTableAvroSchema(false)
                        org.apache.hudi.common.util.Option.of(schema)
                    } catch {
                        case e2: Exception =>
                            logger.error(s"从数据文件获取schema也失败: ${e2.getMessage}", e2)
                            org.apache.hudi.common.util.Option.empty()
                    }
            }

            if (latestAvroSchemaOpt.isPresent) {
                val avroSchema = latestAvroSchemaOpt.get()
                val structType = AvroConversionUtils.convertAvroSchemaToStructType(avroSchema)
                logger.info(s"✓ 成功获取到Hudi表最新schema，字段数: ${structType.fields.length}")
                Some(structType)
            } else {
                logger.error("✗ 无法从事务文件中获取Hudi表schema")
                // 从元数据表中获取schema
                val metaTable = spark.read.format("hudi").load(hudiTablePath)
                val schema = metaTable.schema
                logger.info(s"从元数据表获取到schema，字段数: ${schema.fields.length}")
                Some(schema)
            }

        } catch {
            case e: Exception =>
                logger.error(s"✗ 获取Hudi表最新schema失败: ${e.getMessage}", e)
                e.printStackTrace()
                None
        }
    }

    /**
     * 为指定表的schema添加新字段
     *
     * 该方法会：
     * 1. 从Hudi表中获取最新的schema
     * 2. 从元数据表中读取指定表的当前schema进行对比
     * 3. 解析输入的字段定义字符串
     * 4. 将新字段添加到最新schema中
     * 5. 更新元数据表中的schema记录
     *
     * @param tableId       表ID
     * @param hudiTablePath Hudi表路径，用于获取最新schema
     * @param fieldsToAdd   要添加的字段定义，格式："col1 String, col2 Int, col3 Boolean"
     * @param metaTablePath 元数据表路径
     * @return 是否添加成功
     */
    def addFieldsToSchema(tableId: String, 
                         hudiTablePath: String,
                         fieldsToAdd: String, 
                         metaTablePath: String): Boolean = {
        try {
            logger.info(s"开始为表 $tableId 添加字段: $fieldsToAdd")
            logger.info(s"Hudi表路径: $hudiTablePath")

            // 1. 从Hudi表获取最新的schema
            val latestHudiSchemaOpt = getLatestHudiTableSchema(hudiTablePath)
            if (latestHudiSchemaOpt.isEmpty) {
                logger.error(s"✗ 无法获取Hudi表的最新schema: $hudiTablePath")
                return false
            }
            val latestHudiSchema = latestHudiSchemaOpt.get
            logger.info(s"从Hudi表获取到最新schema，字段数: ${latestHudiSchema.fields.length}")

            // 2. 查询元数据表中的现有记录
            val existingRecord = queryTableMetaById(tableId, metaTablePath).collect()
            
            if (existingRecord.isEmpty) {
                logger.error(s"✗ 未找到表ID为 $tableId 的记录")
                return false
            }

            val record = existingRecord.head
            val currentSchemaJson = record.getAs[String]("schema")
            
            logger.info(s"当前元数据表中的schema: ${currentSchemaJson.take(200)}...")

            // 3. 解析要添加的字段
            val newFields = parseFieldDefinitions(fieldsToAdd)
            logger.info(s"解析到 ${newFields.length} 个新字段")
            
            // 4. 使用最新的Hudi表schema作为基础schema
            val baseSchema = latestHudiSchema
            val existingFieldNames = baseSchema.fieldNames.toSet
            
            // 检查字段名是否重复
            val duplicateFields = newFields.filter(field => existingFieldNames.contains(field.name))
            
            if (duplicateFields.nonEmpty) {
                logger.error(s"✗ 检测到重复字段: ${duplicateFields.map(_.name).mkString(", ")}")
                return false
            }

            // 5. 创建新的schema（基于最新的Hudi表schema）
            val updatedSchema = StructType(baseSchema.fields ++ newFields)
            val updatedSchemaJson = updatedSchema.json
            
            logger.info(s"更新后schema字段数: ${updatedSchema.fields.length}")
            logger.info(s"基于Hudi表最新schema: ${baseSchema.fields.length} 个字段")
            logger.info(s"新增字段: ${newFields.length} 个")

            // 6. 更新元数据记录（支持partial update）
            val currentTime = new java.sql.Timestamp(System.currentTimeMillis())
            
            val updatedData = Seq(
                MetaHudiTableRecord(
                    id = tableId,
                    schema = updatedSchemaJson,
                    status = record.getAs[Int]("status"),
                    is_partitioned = record.getAs[Boolean]("is_partitioned"),
                    partition_expr = Option(record.getAs[String]("partition_expr")),
                    hoodie_config = Option(record.getAs[String]("hoodie_config")),
                    tags = Option(record.getAs[String]("tags")),
                    description = Option(record.getAs[String]("description")),
                    source_db = Option(record.getAs[String]("source_db")),
                    source_table = Option(record.getAs[String]("source_table")),
                    db_type = Option(record.getAs[String]("db_type")),
                    create_time = record.getAs[java.sql.Timestamp]("create_time"),
                    update_time = currentTime
                )
            ).toDF()

            // 写入更新（启用partial update）
            updatedData.write
                .format("hudi")
                .mode("append")
                .save(metaTablePath)

            logger.info(s"✓ 成功为表 $tableId 添加 ${newFields.length} 个字段（基于Hudi表最新schema）")
            newFields.foreach(field => logger.info(s"  - ${field.name}: ${field.dataType}"))
            
            true

        } catch {
            case e: Exception =>
                logger.error(s"✗ 为表添加字段失败: ${e.getMessage}", e)
                false
        }
    }

    /**
     * 解析字段定义字符串
     *
     * @param fieldsDefinition 字段定义字符串，格式："col1 String, col2 Int, col3 Boolean"
     * @return StructField数组
     */
    private def parseFieldDefinitions(fieldsDefinition: String): Array[StructField] = {
        try {
            // 按逗号分割字段定义
            val fieldDefs = fieldsDefinition.split(",").map(_.trim)
            
            fieldDefs.map { fieldDef =>
                // 按空格分割字段名和类型
                val parts = fieldDef.trim.split("\\s+")
                
                if (parts.length < 2) {
                    throw new IllegalArgumentException(s"字段定义格式错误: $fieldDef，期望格式: 'fieldName dataType'")
                }
                
                val fieldName = parts(0).trim
                val dataTypeStr = parts(1).trim
                
                // 解析数据类型
                val dataType = parseDataType(dataTypeStr)
                
                // 默认字段可为空
                StructField(fieldName, dataType, nullable = true)
            }
            
        } catch {
            case e: Exception =>
                logger.error(s"解析字段定义失败: ${e.getMessage}", e)
                throw e
        }
    }

    /**
     * 解析数据类型字符串为Spark DataType
     *
     * @param dataTypeStr 数据类型字符串
     * @return DataType
     */
    private def parseDataType(dataTypeStr: String): DataType = {
        dataTypeStr.toLowerCase.trim match {
            case "string" => StringType
            case "int" | "integer" => IntegerType
            case "long" => LongType
            case "double" => DoubleType
            case "float" => FloatType
            case "boolean" | "bool" => BooleanType
            case "timestamp" => TimestampType
            case "date" => DateType
            case "binary" => BinaryType
            case "byte" => ByteType
            case "short" => ShortType
            case typeStr if typeStr.startsWith("decimal(") =>
                // 解析 decimal(precision, scale) 格式
                val pattern = """decimal\((\d+),\s*(\d+)\)""".r
                typeStr match {
                    case pattern(precision, scale) => DecimalType(precision.toInt, scale.toInt)
                    case _ => throw new IllegalArgumentException(s"无效的decimal格式: $typeStr")
                }
            case typeStr if typeStr.startsWith("varchar(") || typeStr.startsWith("char(") =>
                // varchar和char都映射为StringType
                StringType
            case _ =>
                throw new IllegalArgumentException(s"不支持的数据类型: $dataTypeStr")
        }
    }

    /**
     * 更新表的分区表达式
     *
     * @param tableId       表ID
     * @param partitionExpr 分区表达式，表示如何来计算分区值的
     * @param tablePath     元数据表路径
     * @return 是否更新成功
     */
    def updateTablePartitionExpr(tableId: String, partitionExpr: String, tablePath: String): Boolean = {
        try {
            val currentTime = new java.sql.Timestamp(System.currentTimeMillis())

            // 先查询现有记录
            val existingRecord = queryTableMetaById(tableId, tablePath).collect()

            if (existingRecord.isEmpty) {
                logger.error(s"✗ 未找到表ID为 $tableId 的记录")
                return false
            }

            val record = existingRecord.head

            logger.info(s"更新表 $tableId 的分区表达式:")
            logger.info(s"  原分区表达式: ${Option(record.getAs[String]("partition_expr")).getOrElse("N/A")}")
            logger.info(s"  新分区表达式: ${Option(partitionExpr).getOrElse("N/A")}")

            // 创建更新后的记录
            val updatedData = Seq(
                MetaHudiTableRecord(
                    id = tableId,
                    schema = record.getAs[String]("schema"),
                    status = record.getAs[Int]("status"),
                    is_partitioned = record.getAs[Boolean]("is_partitioned"),
                    partition_expr = Option(partitionExpr),
                    hoodie_config = Option(record.getAs[String]("hoodie_config")),
                    tags = Option(record.getAs[String]("tags")),
                    description = Option(record.getAs[String]("description")),
                    source_db = Option(record.getAs[String]("source_db")),
                    source_table = Option(record.getAs[String]("source_table")),
                    db_type = Option(record.getAs[String]("db_type")),
                    create_time = record.getAs[java.sql.Timestamp]("create_time"),
                    update_time = currentTime
                )
            ).toDF()

            // 写入Hudi表进行更新（支持partial update）
            updatedData.write
                .format("hudi")
                .mode("append")
                .save(tablePath)

            logger.info(s"✓ 表分区表达式更新成功: $tableId")
            true

        } catch {
            case e: Exception =>
                logger.error(s"✗ 更新表分区表达式失败: ${e.getMessage}", e)
                false
        }
    }

    /**
     * 更新表的Hudi配置
     *
     * @param tableId      表ID
     * @param hoodieConfig Hudi配置的JSON串
     * @param tablePath    元数据表路径
     * @return 是否更新成功
     */
    def updateTableHoodieConfig(tableId: String, hoodieConfig: String, tablePath: String): Boolean = {
        try {
            val currentTime = new java.sql.Timestamp(System.currentTimeMillis())

            // 先查询现有记录
            val existingRecord = queryTableMetaById(tableId, tablePath).collect()

            if (existingRecord.isEmpty) {
                logger.error(s"✗ 未找到表ID为 $tableId 的记录")
                return false
            }

            val record = existingRecord.head

            logger.info(s"更新表 $tableId 的Hudi配置:")
            logger.info(s"  原Hudi配置: ${Option(record.getAs[String]("hoodie_config")).getOrElse("N/A")}")
            logger.info(s"  新Hudi配置: ${Option(hoodieConfig).map(_.take(100)).getOrElse("N/A")}...")

            // 创建更新后的记录
            val updatedData = Seq(
                MetaHudiTableRecord(
                    id = tableId,
                    schema = record.getAs[String]("schema"),
                    status = record.getAs[Int]("status"),
                    is_partitioned = record.getAs[Boolean]("is_partitioned"),
                    partition_expr = Option(record.getAs[String]("partition_expr")),
                    hoodie_config = Option(hoodieConfig),
                    tags = Option(record.getAs[String]("tags")),
                    description = Option(record.getAs[String]("description")),
                    source_db = Option(record.getAs[String]("source_db")),
                    source_table = Option(record.getAs[String]("source_table")),
                    db_type = Option(record.getAs[String]("db_type")),
                    create_time = record.getAs[java.sql.Timestamp]("create_time"),
                    update_time = currentTime
                )
            ).toDF()

            // 写入Hudi表进行更新（支持partial update）
            updatedData.write
                .format("hudi")
                .mode("append")
                .save(tablePath)

            logger.info(s"✓ 表Hudi配置更新成功: $tableId")
            true

        } catch {
            case e: Exception =>
                logger.error(s"✗ 更新表Hudi配置失败: ${e.getMessage}", e)
                false
        }
    }

    /**
     * 同时更新表的分区表达式和Hudi配置
     *
     * @param tableId       表ID
     * @param partitionExpr 分区表达式，表示如何来计算分区值的
     * @param hoodieConfig  Hudi配置的JSON串
     * @param tablePath     元数据表路径
     * @return 是否更新成功
     */
    def updateTablePartitionAndHoodieConfig(tableId: String, partitionExpr: String, hoodieConfig: String, tablePath: String): Boolean = {
        try {
            val currentTime = new java.sql.Timestamp(System.currentTimeMillis())

            // 先查询现有记录
            val existingRecord = queryTableMetaById(tableId, tablePath).collect()

            if (existingRecord.isEmpty) {
                logger.error(s"✗ 未找到表ID为 $tableId 的记录")
                return false
            }

            val record = existingRecord.head

            logger.info(s"更新表 $tableId 的分区表达式和Hudi配置:")
            logger.info(s"  原分区表达式: ${Option(record.getAs[String]("partition_expr")).getOrElse("N/A")}")
            logger.info(s"  新分区表达式: ${Option(partitionExpr).getOrElse("N/A")}")
            logger.info(s"  原Hudi配置: ${Option(record.getAs[String]("hoodie_config")).getOrElse("N/A")}")
            logger.info(s"  新Hudi配置: ${Option(hoodieConfig).map(_.take(100)).getOrElse("N/A")}...")

            // 创建更新后的记录
            val updatedData = Seq(
                MetaHudiTableRecord(
                    id = tableId,
                    schema = record.getAs[String]("schema"),
                    status = record.getAs[Int]("status"),
                    is_partitioned = record.getAs[Boolean]("is_partitioned"),
                    partition_expr = Option(partitionExpr),
                    hoodie_config = Option(hoodieConfig),
                    tags = Option(record.getAs[String]("tags")),
                    description = Option(record.getAs[String]("description")),
                    source_db = Option(record.getAs[String]("source_db")),
                    source_table = Option(record.getAs[String]("source_table")),
                    db_type = Option(record.getAs[String]("db_type")),
                    create_time = record.getAs[java.sql.Timestamp]("create_time"),
                    update_time = currentTime
                )
            ).toDF()

            // 写入Hudi表进行更新（支持partial update）
            updatedData.write
                .format("hudi")
                .mode("append")
                .save(tablePath)

            logger.info(s"✓ 表分区表达式和Hudi配置更新成功: $tableId")
            true

        } catch {
            case e: Exception =>
                logger.error(s"✗ 更新表分区表达式和Hudi配置失败: ${e.getMessage}", e)
                false
        }
    }

    /**
     * 根据分区表达式查询表列表
     *
     * @param partitionExpr 分区表达式
     * @param tablePath     元数据表路径
     * @return DataFrame包含指定分区表达式的表记录
     */
    def queryTablesByPartitionExpr(partitionExpr: String, tablePath: String): DataFrame = {
        try {
            queryTableMeta(tablePath)
                .filter($"partition_expr" === partitionExpr)
                .orderBy($"update_time".desc)

        } catch {
            case e: Exception =>
                logger.error(s"根据分区表达式查询表列表失败: ${e.getMessage}", e)
                spark.emptyDataFrame
        }
    }

    /**
     * 查询所有有分区表达式的表
     *
     * @param tablePath 元数据表路径
     * @return DataFrame包含所有有分区表达式的表记录
     */
    def queryTablesWithPartitionExpr(tablePath: String): DataFrame = {
        try {
            queryTableMeta(tablePath)
                .filter($"partition_expr".isNotNull)
                .orderBy($"update_time".desc)

        } catch {
            case e: Exception =>
                logger.error(s"查询有分区表达式的表列表失败: ${e.getMessage}", e)
                spark.emptyDataFrame
        }
    }

    /**
     * 查询所有有Hudi配置的表
     *
     * @param tablePath 元数据表路径
     * @return DataFrame包含所有有Hudi配置的表记录
     */
    def queryTablesWithHoodieConfig(tablePath: String): DataFrame = {
        try {
            queryTableMeta(tablePath)
                .filter($"hoodie_config".isNotNull)
                .orderBy($"update_time".desc)

        } catch {
            case e: Exception =>
                logger.error(s"查询有Hudi配置的表列表失败: ${e.getMessage}", e)
                spark.emptyDataFrame
        }
    }

    /**
     * 从resources目录加载Hudi默认配置
     *
     * @return 默认配置的Map，如果加载失败则返回空Map
     */
    private def loadHoodieDefaultConfig(): Map[String, String] = {
        try {
            // 从classpath加载配置文件
            val configStream = getClass.getResourceAsStream("/hoodie-default.json")
            if (configStream != null) {
                val source = Source.fromInputStream(configStream)
                val configContent = source.mkString
                source.close()
                
                // 解析JSON配置
                val configMap = JSON.parseObject(configContent).asScala.toMap.map {
                    case (k, v) => k -> v.toString
                }
                
                logger.info(s"✓ 成功加载Hudi默认配置，配置项数量: ${configMap.size}")
                configMap
            } else {
                logger.error("✗ 无法找到hoodie-default.json配置文件")
                Map.empty[String, String]
            }
        } catch {
            case e: Exception =>
                logger.error(s"✗ 加载Hudi默认配置失败: ${e.getMessage}", e)
                Map.empty[String, String]
        }
    }

    /**
     * 替换配置模板中的变量
     *
     * @param configMap 配置Map
     * @param variables 变量替换Map
     * @return 替换后的配置Map
     */
    private def replaceConfigVariables(configMap: Map[String, String], variables: Map[String, String]): Map[String, String] = {
        configMap.map { case (key, value) =>
            var replacedValue = value
            variables.foreach { case (varName, varValue) =>
                replacedValue = replacedValue.replace(s"$${$varName}", varValue)
            }
            key -> replacedValue
        }
    }

    /**
     * 生成Hudi配置JSON字符串
     *
     * @param primaryKey        主键字段名
     * @param hmsServerAddress  HMS服务器地址
     * @return Hudi配置JSON字符串
     */
    private def generateHoodieConfig(primaryKey: String, hmsServerAddress: String): String = {
        try {
            // 加载默认配置
            val defaultConfig = loadHoodieDefaultConfig()
            
            if (defaultConfig.isEmpty) {
                logger.error("✗ 使用硬编码的默认配置")
                // 如果加载失败，返回硬编码的默认配置
                return s"""
                {
                    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
                    "hoodie.table.keygenerator.class": "org.apache.hudi.keygen.SimpleKeyGenerator",
                    "hoodie.table.recordkey.fields": "$primaryKey",
                    "hoodie.datasource.write.operation": "upsert",
                    "hoodie.datasource.insert.dup.policy": "insert",
                    "hoodie.datasource.meta.sync.enable": "true",
                    "hoodie.datasource.meta_sync.condition.sync": "true",
                    "hoodie.datasource.hive_sync.enable": "true",
                    "hoodie.datasource.hive_sync.mode": "hms",
                    "hoodie.datasource.hive_sync.metastore.uris": "$hmsServerAddress",
                    "hoodie.datasource.write.precombine.field": "update_time",
                    "hoodie.datasource.write.partitionpath.field": "cdc_dt",
                    "hoodie.datasource.write.hive_style_partitioning": "true",
                    "hoodie.index.type": "BUCKET",
                    "hoodie.bucket.index.hash.field": "id",
                    "hoodie.index.bucket.engine": "SIMPLE",
                    "hoodie.bucket.index.num.buckets": "20",
                    "hoodie.cleaner.commits.retained": "24",
                    "hoodie.insert.shuffle.parallelism": "10",
                    "hoodie.upsert.shuffle.parallelism": "10",
                    "hoodie.bulkinsert.shuffle.parallelism": "500"
                }
                """
            }
            
            // 定义变量替换
            val variables = Map(
                "primaryKey" -> primaryKey,
                "hmsServerAddress" -> hmsServerAddress
            )
            
            // 替换配置中的变量
            val finalConfig = replaceConfigVariables(defaultConfig, variables)
            
            // 转换为JSON字符串
            val jsonConfig = JSON.toJSONString(finalConfig.asJava, SerializerFeature.DisableCircularReferenceDetect)
            
            logger.info(s"✓ 成功生成Hudi配置，主键: $primaryKey")
            logger.info(s"  HMS地址: $hmsServerAddress")
            
            jsonConfig
            
        } catch {
            case e: Exception =>
                logger.error(s"✗ 生成Hudi配置失败: ${e.getMessage}", e)
                "{}"
        }
    }

    /**
     * 基于TDSQL表的schema，自动创建并插入元数据记录到meta_hudi_table中
     *
     * 该方法会：
     * 1. 连接TDSQL数据库
     * 2. 获取指定表的schema信息
     * 3. 将TDSQL数据类型映射为Spark/Hudi数据类型
     * 4. 生成schema JSON
     * 5. 插入到元数据表中
     *
     * @param tdsqlUrl        TDSQL数据库连接URL（如：jdbc:mysql://host:port/database）
     * @param tdsqlUser       TDSQL数据库用户名
     * @param tdsqlPassword   TDSQL数据库密码
     * @param tdsqlDatabase   TDSQL源数据库名称
     * @param tdsqlTable      TDSQL源表名称
     * @param tableId         在元数据表中的表标识符（如果为null，则使用tdsqlTable）
     * @param status          表状态，默认为0（未上线）
     * @param isPartitioned   是否为分区表，默认为false
     * @param partitionExpr   分区表达式，默认为null
     * @param tags            表标签，多个标签用逗号分隔
     * @param description     表描述信息
     * @param metaTablePath   元数据表的存储路径
     * @return 是否操作成功
     */
    def insertTableMetaFromTdsql(tdsqlUrl: String,
                                tdsqlUser: String,
                                tdsqlPassword: String,
                                tdsqlDatabase: String,
                                tdsqlTable: String,
                                tableId: String = null,
                                status: Int = 0,
                                isPartitioned: Boolean = true,
                                partitionExpr: String = "trunc(create_time, 'year')",
                                tags: String = null,
                                description: String = null,
                                metaTablePath: String): Boolean = {

        var connection: Connection = null
        try {
            logger.info(s"开始从TDSQL表获取schema并插入元数据表:")
            logger.info(s"  TDSQL URL: $tdsqlUrl")
            logger.info(s"  源数据库: $tdsqlDatabase")
            logger.info(s"  源表名: $tdsqlTable")
            
            // 获取HMS地址
            val hmsServerAddress = spark.conf.get("spark.hive.metastore.uris")

            // 创建数据库连接
            val properties = new Properties()
            properties.setProperty("user", tdsqlUser)
            properties.setProperty("password", tdsqlPassword)
            properties.setProperty("useSSL", "false")
            properties.setProperty("serverTimezone", "UTC")
            properties.setProperty("characterEncoding", "utf8")
            
            Class.forName("com.mysql.cj.jdbc.Driver")
            val connection = DriverManager.getConnection(tdsqlUrl, properties)

            // 获取TDSQL表的schema
            val tdsqlSchemaOpt = getTdsqlTableSchema(connection, tdsqlDatabase, tdsqlTable)
            
            if (tdsqlSchemaOpt.isEmpty) {
                logger.error(s"✗ 无法获取TDSQL表schema: $tdsqlDatabase.$tdsqlTable")
                return false
            }

            val tdsqlSchema = tdsqlSchemaOpt.get
            logger.info(s"✓ 成功获取TDSQL表schema，字段数: ${tdsqlSchema.fields.length}")

            // 转换schema为JSON
            val schemaJson = tdsqlSchema.json
            logger.info(s"生成的schema JSON: ${schemaJson.take(200)}...")

            // 使用表名作为ID（如果没有指定tableId）
            val finalTableId = Option(tableId).getOrElse(s"${tdsqlDatabase}_${tdsqlTable}")
            
            val primaryKey = getTdsqlTablePrimaryKey(connection, tdsqlDatabase, tdsqlTable)

            // 使用新的配置生成方法，从JSON文件加载配置
            val hoodieConfig = generateHoodieConfig(primaryKey, hmsServerAddress)

            // 插入元数据记录
            val success = insertTableMeta(
                tableId = finalTableId,
                schemaJson = schemaJson,
                status = status,
                isPartitioned = isPartitioned,
                partitionExpr = partitionExpr,
                hoodieConfig = hoodieConfig,
                tags = tags,
                description = description,
                sourceDb = tdsqlDatabase,
                sourceTable = tdsqlTable,
                dbType = "TDSQL",   
                tablePath = metaTablePath
            )

            if (success) {
                logger.info(s"✓ 成功从TDSQL表创建元数据记录: $finalTableId")
                logger.info(s"  源库表: $tdsqlDatabase.$tdsqlTable")
                logger.info(s"  字段数: ${tdsqlSchema.fields.length}")
                logger.info(s"  分区表: $isPartitioned")
            } else {
                logger.error(s"✗ 从TDSQL表创建元数据记录失败: $finalTableId")
            }

            success
        } catch {
            case e: Exception =>
                logger.error(s"✗ 从TDSQL表插入元数据记录失败: ${e.getMessage}", e)
                false
        } finally {
            if (connection != null) connection.close()
        }

    }


    private def getTdsqlTablePrimaryKey(connection: Connection, tdsqlDatabase: String, tdsqlTable: String): String = {  
        val sql = s"""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_KEY = 'PRI'
        """
        val statement = connection.prepareStatement(sql)
        statement.setString(1, tdsqlDatabase)
        statement.setString(2, tdsqlTable)
        val resultSet = statement.executeQuery()
        resultSet.next()
        resultSet.getString("COLUMN_NAME").toLowerCase
    }   

    /**
     * 获取TDSQL表的schema信息
     *
     * @param tdsqlUrl      TDSQL数据库连接URL
     * @param tdsqlUser     TDSQL数据库用户名
     * @param tdsqlPassword TDSQL数据库密码
     * @param database      数据库名称
     * @param table         表名称
     * @return Option[StructType] 表的schema，如果获取失败则返回None
     */
    private def getTdsqlTableSchema(connection: Connection,
                                   database: String,
                                   table: String): Option[StructType] = {
        var resultSet: ResultSet = null
        
        try {   
            logger.info(s"连接TDSQL数据库获取表schema: $database.$table")
            
            // 查询表结构信息
            val sql = s"""
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    CHARACTER_MAXIMUM_LENGTH,
                    NUMERIC_PRECISION,
                    NUMERIC_SCALE,
                    IS_NULLABLE,
                    COLUMN_DEFAULT,
                    COLUMN_COMMENT
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
            """
            
            val statement = connection.prepareStatement(sql)
            statement.setString(1, database)
            statement.setString(2, table)
            resultSet = statement.executeQuery()
            
            val fields = scala.collection.mutable.ArrayBuffer[StructField]()
            
            while (resultSet.next()) {
                // 将字段名转换为小写
                val columnName = resultSet.getString("COLUMN_NAME").toLowerCase
                val dataType = resultSet.getString("DATA_TYPE")
                val maxLength = resultSet.getInt("CHARACTER_MAXIMUM_LENGTH")
                val precision = resultSet.getInt("NUMERIC_PRECISION")
                val scale = resultSet.getInt("NUMERIC_SCALE")
                val isNullable = resultSet.getString("IS_NULLABLE").equalsIgnoreCase("YES")
                val columnComment = resultSet.getString("COLUMN_COMMENT")
                
                // 映射TDSQL数据类型到Spark数据类型
                val sparkDataType = mapTdsqlDataTypeToSpark(dataType, maxLength, precision, scale)
                
                // 创建StructField，添加注释信息作为metadata
                val metadata = if (columnComment != null && columnComment.trim.nonEmpty) {
                    new MetadataBuilder().putString("comment", columnComment).build()
                } else {
                    Metadata.empty
                }
                
                val field = StructField(columnName, sparkDataType, isNullable, metadata)
                fields += field
                
                logger.info(s"  字段: $columnName, TDSQL类型: $dataType -> Spark类型: $sparkDataType, 可空: $isNullable")
            }
            
            if (fields.nonEmpty) {
                val schema = StructType(fields.toArray)
                logger.info(s"✓ 成功解析TDSQL表schema，共 ${fields.length} 个字段")
                Some(schema)
            } else {
                logger.error(s"✗ 未找到表字段信息: $database.$table")
                None
            }
            
        } catch {
            case e: Exception =>
                logger.error(s"✗ 获取TDSQL表schema失败: ${e.getMessage}", e)
                None
        } 
    }

    /**
     * 将TDSQL数据类型映射为Spark数据类型
     *
     * @param tdsqlDataType TDSQL数据类型
     * @param maxLength     最大长度（用于字符串类型）
     * @param precision     精度（用于数值类型）
     * @param scale         小数位数（用于数值类型）
     * @return Spark DataType
     */
    private def mapTdsqlDataTypeToSpark(tdsqlDataType: String, 
                                       maxLength: Int, 
                                       precision: Int, 
                                       scale: Int): DataType = {
        tdsqlDataType.toLowerCase.trim match {
            // 整数类型
            case "tinyint" => if (precision == 1) BooleanType else ByteType
            case "smallint" => ShortType
            case "mediumint" | "int" | "integer" => IntegerType
            case "bigint" => LongType
            
            // 浮点数类型
            case "float" => FloatType
            case "double" | "double precision" => DoubleType
            case "real" => FloatType
            
            // 定点数类型
            case "decimal" | "numeric" => 
                if (precision > 0 && scale >= 0) {
                    DecimalType(precision, scale)
                } else {
                    DecimalType(10, 0) // 默认精度
                }
            
            // 字符串类型
            case "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" => StringType
            case "enum" | "set" => StringType
            
            // 二进制类型
            case "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" => BinaryType
            
            // 日期时间类型
            case "date" => DateType
            case "time" => StringType // MySQL TIME类型映射为字符串
            case "datetime" | "timestamp" => TimestampType
            case "year" => IntegerType
            
            // JSON类型（MySQL 5.7+）
            case "json" => StringType
            
            // 几何类型
            case "geometry" | "point" | "linestring" | "polygon" | "multipoint" | 
                 "multilinestring" | "multipolygon" | "geometrycollection" => StringType
            
            // 位类型
            case "bit" => 
                if (precision == 1) BooleanType else BinaryType
            
            // 其他未知类型默认为字符串
            case _ => 
                logger.warn(s"警告: 未知的TDSQL数据类型 '$tdsqlDataType'，映射为StringType")
                StringType
        }
    }

    /**
     * 根据table_id生成Hudi表的DDL语句
     *
     * 该方法会：
     * 1. 根据table_id查询表的元数据信息
     * 2. 解析schema JSON为StructType
     * 3. 根据元数据中的配置生成完整的Spark SQL DDL语句
     * 4. 支持分区表和非分区表的DDL生成
     * 5. 包含完整的TBLPROPERTIES配置
     *
     * @param tableId       表ID
     * @param tablePath     目标表的存储路径
     * @param metaTablePath 元数据表路径
     * @return DDL字符串，如果失败则返回None
     */
    def generateHudiTableDDL(tableId: String, tablePath: String, metaTablePath: String): Option[String] = {
        try {
            logger.info(s"开始为表 $tableId 生成Hudi表DDL...")

            // 1. 查询表的元数据信息
            val metaRecord = queryTableMetaById(tableId, metaTablePath).collect()
            
            if (metaRecord.isEmpty) {
                logger.error(s"✗ 未找到表ID为 $tableId 的元数据记录")
                return None
            }

            val record = metaRecord.head
            val schemaJson = record.getAs[String]("schema")
            val isPartitioned = record.getAs[Boolean]("is_partitioned")
            val partitionExpr = Option(record.getAs[String]("partition_expr"))
            val hoodieConfig = Option(record.getAs[String]("hoodie_config"))
            val description = Option(record.getAs[String]("description"))
            val sourceDb = Option(record.getAs[String]("source_db"))
            val sourceTable = Option(record.getAs[String]("source_table"))
            val dbType = Option(record.getAs[String]("db_type"))

            logger.info(s"✓ 成功获取表元数据: $tableId")
            logger.info(s"  分区表: $isPartitioned")
            logger.info(s"  源库表: ${sourceDb.getOrElse("N/A")}.${sourceTable.getOrElse("N/A")}")
            logger.info(s"  数据库类型: ${dbType.getOrElse("N/A")}")

            // 2. 解析schema JSON为StructType
            val schema = try {
                DataType.fromJson(schemaJson).asInstanceOf[StructType]
            } catch {
                case e: Exception =>
                    logger.error(s"✗ 解析schema JSON失败: ${e.getMessage}", e)
                    return None
            }

            logger.info(s"✓ 成功解析schema，字段数: ${schema.fields.length}")

            // 3. 生成DDL语句
            val ddl = buildHudiTableDDL(
                tableId = tableId,
                schema = schema,
                tablePath = tablePath,
                isPartitioned = isPartitioned,
                partitionExpr = partitionExpr,
                hoodieConfig = hoodieConfig,
                description = description
            )

            logger.info(s"✓ 成功生成Hudi表DDL: $tableId")
            Some(ddl)

        } catch {
            case e: Exception =>
                logger.error(s"✗ 生成Hudi表DDL失败: ${e.getMessage}", e)
                None
        }
    }

    /**
     * 构建Hudi表的DDL语句
     *
     * @param tableId       表ID
     * @param schema        表schema
     * @param tablePath     表存储路径
     * @param isPartitioned 是否分区表
     * @param partitionExpr 分区表达式
     * @param hoodieConfig  Hudi配置JSON
     * @param description   表描述
     * @return DDL字符串
     */
    private def buildHudiTableDDL(tableId: String,
                                  schema: StructType,
                                  tablePath: String,
                                  isPartitioned: Boolean,
                                  partitionExpr: Option[String],
                                  hoodieConfig: Option[String],
                                  description: Option[String]): String = {
        
        // 1. 构建字段定义
        val fieldDefinitions = schema.fields.map { field =>
            val fieldName = field.name
            val dataType = sparkDataTypeToSqlString(field.dataType)
            val nullable = if (field.nullable) "" else " NOT NULL"
            val comment = field.metadata.getString("comment") match {
                case commentStr if commentStr != null && commentStr.trim.nonEmpty => s" COMMENT '$commentStr'"
                case _ => ""
            }
            s"    $fieldName $dataType$nullable$comment"
        }.mkString(",\n")

        // 2. 构建表注释
        val tableComment = description match {
            case Some(desc) if desc.trim.nonEmpty => s"\nCOMMENT '$desc'"
            case _ => ""
        }

        // 3. 构建分区定义
        val partitionClause = if (isPartitioned) {
            // 使用cdc_dt作为分区字段
            s"\nPARTITIONED BY (cdc_dt String)"
        } else {
            ""
        }

        // 4. 构建TBLPROPERTIES
        val tblProperties = buildTblProperties(tableId, hoodieConfig, isPartitioned)

        // 5. 构建完整的DDL
        val ddl = s"""CREATE TABLE IF NOT EXISTS $tableId (
                     |$fieldDefinitions
                     |) USING HUDI$tableComment
                     |LOCATION '$tablePath'$partitionClause
                     |$tblProperties
                     |""".stripMargin

        ddl
    }

    /**
     * 将Spark DataType转换为SQL字符串表示
     *
     * @param dataType Spark DataType
     * @return SQL数据类型字符串
     */
    private def sparkDataTypeToSqlString(dataType: DataType): String = {
        dataType match {
            case StringType => "STRING"
            case IntegerType => "INT"
            case LongType => "BIGINT"
            case DoubleType => "DOUBLE"
            case FloatType => "FLOAT"
            case BooleanType => "BOOLEAN"
            case TimestampType => "TIMESTAMP"
            case DateType => "DATE"
            case BinaryType => "BINARY"
            case ByteType => "TINYINT"
            case ShortType => "SMALLINT"
            case dt: DecimalType => s"DECIMAL(${dt.precision},${dt.scale})"
            case _ => dataType.sql
        }
    }

    /**
     * 从分区表达式中提取分区字段
     *
     * @param partitionExpr 分区表达式
     * @return 分区字段名
     */
    private def extractPartitionFieldFromExpr(partitionExpr: Option[String]): String = {
        partitionExpr match {
            case Some(expr) if expr.trim.nonEmpty =>
                // 简单的分区字段提取逻辑，可以根据需要扩展
                if (expr.contains("cdc_dt")) {
                    "cdc_dt"
                } else if (expr.contains("create_time")) {
                    "create_time"
                } else if (expr.contains("update_time")) {
                    "update_time"
                } else {
                    // 默认使用cdc_dt
                    "cdc_dt"
                }
            case _ => "cdc_dt" // 默认分区字段
        }
    }

    /**
     * 构建TBLPROPERTIES配置
     *
     * @param tableId       表ID
     * @param hoodieConfig  Hudi配置JSON
     * @param isPartitioned 是否分区表
     * @return TBLPROPERTIES字符串
     */
    private def buildTblProperties(tableId: String, hoodieConfig: Option[String], isPartitioned: Boolean): String = {
        
        // 默认的Hudi配置
        val defaultProperties = Map(
            "hoodie.datasource.write.table.type" -> "COPY_ON_WRITE",
            "hoodie.datasource.write.keygenerator.class" -> "org.apache.hudi.keygen.SimpleKeyGenerator",
            "hoodie.table.recordkey.fields" -> "id",
            "hoodie.datasource.write.operation" -> "upsert",
            "hoodie.datasource.insert.dup.policy" -> "insert",
            "hoodie.datasource.write.precombine.field" -> "update_time",
            "hoodie.index.type" -> "BUCKET",
            "hoodie.index.bucket.engine" -> "SIMPLE",
            "hoodie.bucket.index.num.buckets" -> "20",
            "hoodie.cleaner.commits.retained" -> "24",
            "hoodie.insert.shuffle.parallelism" -> "10",
            "hoodie.upsert.shuffle.parallelism" -> "10",
            "hoodie.bulkinsert.shuffle.parallelism" -> "10"
        )

        // 如果是分区表，添加分区相关配置
        val partitionProperties = if (isPartitioned) {
            Map(
                "hoodie.datasource.write.partitionpath.field" -> "cdc_dt",
                "hoodie.datasource.write.hive_style_partitioning" -> "true"
            )
        } else {
            Map.empty[String, String]
        }

        // 合并配置
        var allProperties = partitionProperties
        // 判断hoodieConfig是否为空
        if (hoodieConfig.isDefined && hoodieConfig.get.nonEmpty) {
            // 使用FastJSON解析hoodieConfig json字符串为Map结构
            import com.alibaba.fastjson.JSON
            import scala.collection.JavaConverters._        
            
            val hoodieConfigMap = JSON.parseObject(hoodieConfig.get).asScala.toMap.map {
                case (k, v) => k -> v.toString
            }
            allProperties = defaultProperties ++ partitionProperties ++ hoodieConfigMap
        } else {
            allProperties = defaultProperties ++ partitionProperties
        }

        // 构建TBLPROPERTIES字符串
        val propertiesStr = allProperties.map {
            case (key, value) => s"    '$key' = '$value'"
        }.mkString(",\n")

        s"""TBLPROPERTIES (
           |$propertiesStr
           |)""".stripMargin
    }

    /**
     * 生成并打印Hudi表的DDL语句
     *
     * @param tableId       表ID
     * @param tablePath     目标表的存储路径
     * @param metaTablePath 元数据表路径                                                                                                                                             
     * @return DDL语句字符串，失败时返回空字符串
     */
    def generateAndPrintHudiTableDDL(tableId: String, tablePath: String, metaTablePath: String): String = {
        try {
            val ddlOpt = generateHudiTableDDL(tableId, tablePath, metaTablePath)
            
            ddlOpt match {
                case Some(ddl) =>
                    logger.info(s"\n=== Hudi表 $tableId 的DDL语句 ===")
                    logger.info(ddl)
                    logger.info("=" * 50)
                    ddl
                case None =>
                    logger.error(s"✗ 无法生成表 $tableId 的DDL语句")
                    ""
            }
            
        } catch {
            case e: Exception =>
                logger.error(s"✗ 生成并打印DDL失败: ${e.getMessage}", e)
                ""
        }
    }
}

/**
 * MetaHudiTableManager的伴生对象，提供便捷的工厂方法
 */
object MetaHudiTableManager {

    /**
     * 创建MetaHudiTableManager实例的工厂方法
     *
     * @param spark SparkSession实例
     * @return MetaHudiTableManager实例
     */
    def apply(spark: SparkSession): MetaHudiTableManager = {
        new MetaHudiTableManager(spark)
    }

    /**
     * 表状态常量定义
     */
    object TableStatus {
        val OFFLINE = 0 // 未上线
        val ONLINE = 1 // 已上线
    }

    /**
     * 默认元数据表路径
     */
    val DEFAULT_META_TABLE_PATH = "/tmp/hudi_tables/meta_hudi_table"
}


/**
 * 元数据表记录的case class定义
 */
case class MetaHudiTableRecord(
                                  id: String,
                                  schema: String,
                                  status: Int,
                                  is_partitioned: Boolean,
                                  partition_expr: Option[String],
                                  hoodie_config: Option[String],
                                  tags: Option[String],
                                  description: Option[String],
                                  source_db: Option[String],
                                  source_table: Option[String],
                                  db_type: Option[String],
                                  create_time: java.sql.Timestamp,
                                  update_time: java.sql.Timestamp
                              )