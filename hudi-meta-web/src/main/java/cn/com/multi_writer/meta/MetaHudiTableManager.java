package cn.com.multi_writer.meta;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

/**
 * Hudi表元数据管理器 - Java版本
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
 * MetaHudiTableManager metaManager = new MetaHudiTableManager(spark);
 * metaManager.createMetaTable();
 * metaManager.insertTableMeta("t_alc_loan", schemaJson, 1, "业务表,核心表", "贷款信息表", "finance_db", "loan_info", "MySQL", tablePath);
 */
public class MetaHudiTableManager {
    
    private static final Logger logger = LoggerFactory.getLogger(MetaHudiTableManager.class);
    
    protected final SparkSession spark;
    
    /**
     * 表状态常量定义
     */
    public static class TableStatus {
        public static final int OFFLINE = 0; // 未上线
        public static final int ONLINE = 1;  // 已上线
    }
    
    /**
     * 默认元数据表路径
     */
    public static final String DEFAULT_META_TABLE_PATH = "/tmp/hudi_tables/meta_hudi_table";
    
    /**
     * 构造函数
     *
     * @param spark SparkSession实例
     */
    public MetaHudiTableManager(SparkSession spark) {
        this.spark = spark;
    }
    
    /**
     * 从资源文件中读取DDL语句
     *
     * @param resourcePath 资源文件路径
     * @return DDL字符串内容
     * @throws IOException 读取文件异常
     */
    private String readDDLFromResource(String resourcePath) throws IOException {
        StringBuilder content = new StringBuilder();
        
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(resourcePath);
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            
            if (inputStream == null) {
                throw new IOException("DDL资源文件未找到: " + resourcePath);
            }
            
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line).append("\n");
            }
            
        } catch (IOException e) {
            logger.error("读取DDL资源文件失败: {}", e.getMessage());
            throw e;
        }
        
        return content.toString().trim();
    }

    /**
     * 创建元数据表的Spark SQL DDL列表
     *
     * @return DDL字符串列表，支持多个DDL语句依次执行
     */
    public List<String> getCreateHoodieMetaDDL() {
        try {
            // 从资源文件中读取DDL内容
            String ddlContent = readDDLFromResource("_hoodie_meta_hudi_hudi.ddl");
            
            // 解析多个DDL语句
            return parseDDLStatements(ddlContent);
            
        } catch (IOException e) {
            logger.error("读取DDL模板失败: {}", e.getMessage());
            throw new RuntimeException("读取DDL模板失败", e);
        }
    }
    
    /**
     * 解析DDL文件中的多个语句
     *
     * @param ddlContent DDL文件内容
     * @return 解析后的DDL语句列表
     */
    private List<String> parseDDLStatements(String ddlContent) {
        List<String> ddlStatements = new ArrayList<>();
        
        // 使用分号分割DDL语句
        String[] statements = ddlContent.split(";");
        
        for (String statement : statements) {
            // 去掉注释和空白行
            String cleanStatement = statement.trim();
            if (!cleanStatement.isEmpty() && !cleanStatement.startsWith("--")) {
                ddlStatements.add(cleanStatement);
            }
        }
        
        return ddlStatements;
    }
    
    /**
     * 创建元数据表
     *
     * @return 是否创建成功
     */
    public boolean createMetaTable() {
        try {
            logger.info("开始创建Hudi元数据表: meta_hudi_table");
            
            // 获取DDL语句列表
            List<String> ddlStatements = getCreateHoodieMetaDDL();
            logger.info("共需要执行 {} 个DDL语句", ddlStatements.size());
            
            // 依次执行每个DDL语句
            for (int i = 0; i < ddlStatements.size(); i++) {
                String ddl = ddlStatements.get(i);
                logger.info("执行第 {} 个DDL语句:", i + 1);
                logger.info(ddl);
                
                try {
                    spark.sql(ddl);
                    logger.info("✓ 第 {} 个DDL语句执行成功", i + 1);
                } catch (Exception e) {
                    logger.error("✗ 第 {} 个DDL语句执行失败: {}", i + 1, e.getMessage(), e);
                    // 继续执行下一个DDL语句，不中断整个过程
                }
            }
            
            logger.info("✓ Hudi元数据表创建流程完成，已启用partial update支持");
            return true;
            
        } catch (Exception e) {
            logger.error("✗ 创建Hudi元数据表失败: {}", e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * 插入或更新表元数据记录
     *
     * @param tableId       表标识符
     * @param schemaJson    表结构JSON字符串
     * @param status        表状态（0-未上线，1-已上线）
     * @param partitionExpr 分区表达式，表示如何来计算分区值的
     * @param hoodieConfig  Hudi表配置的JSON串，用于保存创建表时Hudi表的配置
     * @param tags          表标签
     * @param description   表描述
     * @param sourceDb      源数据库名称
     * @param sourceTable   源表名称
     * @param dbType        数据库类型
     * @param tablePath     元数据表路径
     * @return 是否操作成功
     */
    public boolean insertTableMeta(String tableId, String schemaJson, int status, 
                                  String partitionExpr, String hoodieConfig, String tags, 
                                  String description, String sourceDb, String sourceTable, 
                                  String dbType, String tablePath) {
        try {
            Timestamp currentTime = new Timestamp(System.currentTimeMillis());
            
            // 创建记录
            MetaHudiTableRecord record = new MetaHudiTableRecord(
                tableId, schemaJson, status, true, partitionExpr, hoodieConfig,
                tags, description, sourceDb, sourceTable, dbType, currentTime, currentTime, 0
            );
            
            // 创建DataFrame
            List<Row> rows = Collections.singletonList(createRowFromRecord(record));
            StructType schema = createMetaTableSchema();
            Dataset<Row> metaData = spark.createDataFrame(rows, schema);
            
            logger.info("插入元数据记录: {} (分区表达式: {}, 源库: {}, 源表: {}, 数据库类型: {})",
                tableId, 
                partitionExpr != null ? partitionExpr : "N/A",
                sourceDb != null ? sourceDb : "N/A",
                sourceTable != null ? sourceTable : "N/A",
                dbType != null ? dbType : "N/A");
            
            // 写入Hudi表
            metaData.write()
                   .format("hudi")
                   .mode("append")
                   .save(tablePath);
            
            logger.info("✓ 元数据记录插入成功: {}", tableId);
            return true;
            
        } catch (Exception e) {
            logger.error("✗ 插入元数据记录失败: {}", e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * 查询表元数据
     *
     * @param tablePath 元数据表路径
     * @return Dataset包含所有元数据记录
     */
    public Dataset<Row> queryTableMeta(String tablePath) {
        try {
            return spark.read()
                       .format("hudi")
                       .load(tablePath)
                       .select("id", "schema", "status", "is_partitioned", "partition_expr", 
                              "hoodie_config", "tags", "description", "source_db", "source_table", 
                              "db_type", "create_time", "update_time");
        } catch (Exception e) {
            logger.error("查询元数据表失败: {}", e.getMessage(), e);
            return spark.emptyDataFrame();
        }
    }
    
    /**
     * 根据表ID查询特定表的元数据
     *
     * @param tableId   表ID
     * @param tablePath 元数据表路径
     * @return Dataset包含指定表的元数据记录
     */
    public Dataset<Row> queryTableMetaById(String tableId, String tablePath) {
        try {
            return queryTableMeta(tablePath).filter(col("id").equalTo(tableId));
        } catch (Exception e) {
            logger.error("根据ID查询元数据失败: {}", e.getMessage(), e);
            return spark.emptyDataFrame();
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
    public boolean updateTableStatus(String tableId, int newStatus, String tablePath) {
        try {
            Timestamp currentTime = new Timestamp(System.currentTimeMillis());
            
            // 先查询现有记录
            Row[] existingRecord = (Row[]) queryTableMetaById(tableId, tablePath).collect();
            
            if (existingRecord.length == 0) {
                logger.error("✗ 未找到表ID为 {} 的记录", tableId);
                return false;
            }
            
            Row record = existingRecord[0];
            
            // 创建更新后的记录
            MetaHudiTableRecord updatedRecord = new MetaHudiTableRecord(
                tableId,
                record.getString(record.fieldIndex("schema")),
                newStatus,
                record.getBoolean(record.fieldIndex("is_partitioned")),
                record.getString(record.fieldIndex("partition_expr")),
                record.getString(record.fieldIndex("hoodie_config")),
                record.getString(record.fieldIndex("tags")),
                record.getString(record.fieldIndex("description")),
                record.getString(record.fieldIndex("source_db")),
                record.getString(record.fieldIndex("source_table")),
                record.getString(record.fieldIndex("db_type")),
                record.getTimestamp(record.fieldIndex("create_time")),
                currentTime,
                record.getAs(record.fieldIndex("cdc_delete_flag"))
            );
            
            // 创建DataFrame
            List<Row> rows = Collections.singletonList(createRowFromRecord(updatedRecord));
            StructType schema = createMetaTableSchema();
            Dataset<Row> updatedData = spark.createDataFrame(rows, schema);
            
            // 写入Hudi表进行更新（支持partial update）
            updatedData.write()
                      .format("hudi")
                      .mode("append")
                      .save(tablePath);
            
            logger.info("✓ 表状态更新成功: {} -> {}", tableId, newStatus);
            return true;
            
        } catch (Exception e) {
            logger.error("✗ 更新表状态失败: {}", e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * 根据状态查询表列表
     *
     * @param status    表状态（0-未上线，1-已上线）
     * @param tablePath 元数据表路径
     * @return Dataset包含指定状态的表记录
     */
    public Dataset<Row> queryTablesByStatus(int status, String tablePath) {
        try {
            return queryTableMeta(tablePath)
                    .filter(col("status").equalTo(status))
                    .orderBy(col("update_time").desc());
        } catch (Exception e) {
            logger.error("根据状态查询表列表失败: {}", e.getMessage(), e);
            return spark.emptyDataFrame();
        }
    }
    
    /**
     * 根据标签查询表列表
     *
     * @param tag       标签名称
     * @param tablePath 元数据表路径
     * @return Dataset包含指定标签的表记录
     */
    public Dataset<Row> queryTablesByTag(String tag, String tablePath) {
        try {
            return queryTableMeta(tablePath)
                    .filter(col("tags").contains(tag))
                    .orderBy(col("update_time").desc());
        } catch (Exception e) {
            logger.error("根据标签查询表列表失败: {}", e.getMessage(), e);
            return spark.emptyDataFrame();
        }
    }
    
    /**
     * 根据数据库类型查询表列表
     *
     * @param dbType    数据库类型（如：MySQL、PostgreSQL、Oracle等）
     * @param tablePath 元数据表路径
     * @return Dataset包含指定数据库类型的表记录
     */
    public Dataset<Row> queryTablesByDbType(String dbType, String tablePath) {
        try {
            return queryTableMeta(tablePath)
                    .filter(col("db_type").equalTo(dbType))
                    .orderBy(col("update_time").desc());
        } catch (Exception e) {
            logger.error("根据数据库类型查询表列表失败: {}", e.getMessage(), e);
            return spark.emptyDataFrame();
        }
    }
    
    /**
     * 根据源数据库名称查询表列表
     *
     * @param sourceDb  源数据库名称
     * @param tablePath 元数据表路径
     * @return Dataset包含指定源数据库的表记录
     */
    public Dataset<Row> queryTablesBySourceDb(String sourceDb, String tablePath) {
        try {
            return queryTableMeta(tablePath)
                    .filter(col("source_db").equalTo(sourceDb))
                    .orderBy(col("update_time").desc());
        } catch (Exception e) {
            logger.error("根据源数据库查询表列表失败: {}", e.getMessage(), e);
            return spark.emptyDataFrame();
        }
    }
    
    /**
     * 根据源表名称查询表列表（支持模糊匹配）
     *
     * @param sourceTable 源表名称
     * @param tablePath   元数据表路径
     * @param isExact     是否精确匹配，默认为true，false时进行模糊匹配
     * @return Dataset包含匹配源表名称的表记录
     */
    public Dataset<Row> queryTablesBySourceTable(String sourceTable, String tablePath, boolean isExact) {
        try {
            Dataset<Row> result;
            if (isExact) {
                result = queryTableMeta(tablePath)
                        .filter(col("source_table").equalTo(sourceTable));
            } else {
                result = queryTableMeta(tablePath)
                        .filter(col("source_table").contains(sourceTable));
            }
            
            return result.orderBy(col("update_time").desc());
            
        } catch (Exception e) {
            logger.error("根据源表名称查询表列表失败: {}", e.getMessage(), e);
            return spark.emptyDataFrame();
        }
    }
    
    /**
     * 根据源数据库和源表名称查询特定表
     *
     * @param sourceDb    源数据库名称
     * @param sourceTable 源表名称
     * @param tablePath   元数据表路径
     * @return Dataset包含匹配的表记录
     */
    public Dataset<Row> queryTableBySourceDbAndTable(String sourceDb, String sourceTable, String tablePath) {
        try {
            return queryTableMeta(tablePath)
                    .filter(col("source_db").equalTo(sourceDb).and(col("source_table").equalTo(sourceTable)))
                    .orderBy(col("update_time").desc());
        } catch (Exception e) {
            logger.error("根据源数据库和源表查询失败: {}", e.getMessage(), e);
            return spark.emptyDataFrame();
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
    public boolean updateTableOnlineStatus(String tableId, String status, String tablePath) {
        try {
            int statusValue;
            String statusText;
            
            switch (status.toLowerCase().trim()) {
                case "online":
                    statusValue = TableStatus.ONLINE;
                    statusText = "上线";
                    break;
                case "offline":
                    statusValue = TableStatus.OFFLINE;
                    statusText = "下线";
                    break;
                default:
                    logger.error("✗ 无效的状态值: {}，支持的状态: online, offline", status);
                    return false;
            }
            
            logger.info("开始更新表 {} 状态为: {}", tableId, statusText);
            
            boolean result = updateTableStatus(tableId, statusValue, tablePath);
            
            if (result) {
                logger.info("✓ 表 {} 状态更新成功: {}", tableId, statusText);
            } else {
                logger.error("✗ 表 {} 状态更新失败: {}", tableId, statusText);
            }
            
            return result;
            
        } catch (Exception e) {
            logger.error("✗ 更新表状态失败: {}", e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * 设置表为上线状态
     *
     * @param tableId   表ID
     * @param tablePath 元数据表路径
     * @return 是否设置成功
     */
    public boolean setTableOnline(String tableId, String tablePath) {
        return updateTableOnlineStatus(tableId, "online", tablePath);
    }
    
    /**
     * 设置表为下线状态
     *
     * @param tableId   表ID
     * @param tablePath 元数据表路径
     * @return 是否设置成功
     */
    public boolean setTableOffline(String tableId, String tablePath) {
        return updateTableOnlineStatus(tableId, "offline", tablePath);
    }
    
    /**
     * 查询所有表
     *
     * @param tablePath 元数据表路径
     * @return Dataset包含所有表记录
     */
    public Dataset<Row> queryAllTables(String tablePath) {
        try {
            return queryTableMeta(tablePath).orderBy(col("update_time").desc());
        } catch (Exception e) {
            logger.error("查询所有表失败: {}", e.getMessage(), e);
            return spark.emptyDataFrame();
        }
    }
    
    /**
     * 查询所有已上线的Hudi表
     *
     * 该方法返回所有状态为"已上线"(status=1)的表记录，
     * 结果按更新时间倒序排列，便于查看最近上线的表
     *
     * @param tablePath 元数据表路径
     * @return Dataset包含所有已上线表的记录
     */
    public Dataset<Row> queryOnlineTables(String tablePath) {
        try {
            logger.info("开始查询所有已上线的Hudi表...");
            
            Dataset<Row> onlineTables = queryTablesByStatus(TableStatus.ONLINE, tablePath);
            long count = onlineTables.count();
            
            logger.info("✓ 查询完成，共找到 {} 个已上线的表", count);
            
            return onlineTables;
            
        } catch (Exception e) {
            logger.error("✗ 查询已上线表失败: {}", e.getMessage(), e);
            return spark.emptyDataFrame();
        }
    }
    
    /**
     * 从Hudi表路径提取schema并插入到元数据表中
     *
     * @param hudiTablePath Hudi表的存储路径
     * @param metaTablePath 元数据表的存储路径
     * @param status        表状态，默认为0（未上线）
     * @param tags          表标签
     * @param description   表描述信息
     * @param sourceDb      源数据库名称
     * @param sourceTable   源表名称
     * @param dbType        数据库类型
     * @return 是否操作成功
     */
    public boolean extractAndInsertHudiTableSchema(String hudiTablePath, String metaTablePath, 
                                                  int status, String tags, String description, 
                                                  String sourceDb, String sourceTable, String dbType) {
        try {
            logger.info("开始从Hudi表路径提取schema: {}", hudiTablePath);
            
            // 读取Hudi表获取schema
            Dataset<Row> hudiTable = spark.read()
                                         .format("hudi")
                                         .load(hudiTablePath);
            
            // 获取schema并转换为JSON字符串
            // 过滤掉Hudi自带的数据列和cdc_开头的数据列
            StructType originalSchema = hudiTable.schema();
            List<StructField> filteredFields = Arrays.stream(originalSchema.fields())
                    .filter(field -> {
                        String fieldName = field.name();
                        return !fieldName.startsWith("_hoodie_") && !fieldName.startsWith("cdc_");
                    })
                    .collect(Collectors.toList());
            
            // 创建过滤后的schema
            StructType filteredSchema = DataTypes.createStructType(filteredFields);
            String schemaJson = filteredSchema.json();
            logger.info("提取到的schema（已过滤Hudi和CDC列）: {}...", schemaJson.substring(0, Math.min(schemaJson.length(), 200)));
            
            // 检测是否为分区表
            boolean isPartitioned = detectPartitionedTable(hudiTablePath);
            logger.info("检测到分区信息: {}", isPartitioned ? "分区表" : "非分区表");
            
            // 从路径中提取表名作为ID
            String tableId = extractTableNameFromPath(hudiTablePath);
            logger.info("提取的表ID: {}", tableId);
            
            // 插入元数据记录
            boolean success = insertTableMeta(tableId, schemaJson, status, null, null, 
                                            tags, description, sourceDb, sourceTable, dbType, metaTablePath);
            
            if (success) {
                logger.info("✓ 成功从Hudi表提取schema并插入元数据表: {} (分区表: {})", tableId, isPartitioned);
            } else {
                logger.error("✗ 从Hudi表提取schema并插入元数据表失败: {}", tableId);
            }
            
            return success;
            
        } catch (Exception e) {
            logger.error("✗ 从Hudi表提取schema失败: {}", e.getMessage(), e);
            return false;
        }
    }

    /**
     * 更新表元数据 - 支持更新int类型字段
     * 
     * @param tableId    表ID
     * @param columnName 列名（支持：status）
     * @param value      int类型的值
     * @param tablePath  元数据表路径
     * @return 是否更新成功
     */
    public boolean updateTableMeta(String tableId, String columnName, int value, String tablePath) {
        try {
            logger.info("开始更新表元数据: {} 字段: {} 值: {}", tableId, columnName, value);
            
            // 先查询现有记录
            Row[] existingRecord = (Row[]) queryTableMetaById(tableId, tablePath).collect();
            
            if (existingRecord.length == 0) {
                logger.error("✗ 未找到表ID为 {} 的记录", tableId);
                return false;
            }
            
            Row record = existingRecord[0];
            Timestamp currentTime = new Timestamp(System.currentTimeMillis());
            
            // 根据字段名更新对应的值
            MetaHudiTableRecord updatedRecord;
            
            switch (columnName.toLowerCase()) {
                case "status":
                    updatedRecord = new MetaHudiTableRecord(
                        tableId,
                        record.getString(record.fieldIndex("schema")),
                        value, // 更新status字段
                        record.getBoolean(record.fieldIndex("is_partitioned")),
                        record.getString(record.fieldIndex("partition_expr")),
                        record.getString(record.fieldIndex("hoodie_config")),
                        record.getString(record.fieldIndex("tags")),
                        record.getString(record.fieldIndex("description")),
                        record.getString(record.fieldIndex("source_db")),
                        record.getString(record.fieldIndex("source_table")),
                        record.getString(record.fieldIndex("db_type")),
                        record.getTimestamp(record.fieldIndex("create_time")),
                        currentTime,
                        record.getAs(record.fieldIndex("cdc_delete_flag"))
                    );
                    break;
                case "cdc_delete_flag":
                    updatedRecord = new MetaHudiTableRecord(
                        tableId,
                        record.getString(record.fieldIndex("schema")),
                        record.getAs(record.fieldIndex("status")),
                        record.getBoolean(record.fieldIndex("is_partitioned")),
                        record.getString(record.fieldIndex("partition_expr")),
                        record.getString(record.fieldIndex("hoodie_config")),
                        record.getString(record.fieldIndex("tags")),
                        record.getString(record.fieldIndex("description")),
                        record.getString(record.fieldIndex("source_db")),
                        record.getString(record.fieldIndex("source_table")),
                        record.getString(record.fieldIndex("db_type")),
                        record.getTimestamp(record.fieldIndex("create_time")),
                        currentTime,
                        value
                    );
                    break;
                default:
                    logger.error("✗ 不支持的int类型字段: {}，支持的字段: status", columnName);
                    return false;
            }
            
            // 创建DataFrame
            List<Row> rows = Collections.singletonList(createRowFromRecord(updatedRecord));
            StructType schema = createMetaTableSchema();
            Dataset<Row> updatedData = spark.createDataFrame(rows, schema);
            
            // 写入Hudi表进行更新（支持partial update）
            updatedData.write()
                      .format("hudi")
                      .mode("append")
                      .save(tablePath);
            
            logger.info("✓ 表元数据更新成功: {} 字段: {} 值: {}", tableId, columnName, value);
            return true;
            
        } catch (Exception e) {
            logger.error("✗ 更新表元数据失败: {}", e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * 更新表元数据 - 支持更新String类型字段
     * 
     * @param tableId    表ID
     * @param columnName 列名（支持：schema, partition_expr, hoodie_config, tags, description, source_db, source_table, db_type）
     * @param value      String类型的值
     * @param tablePath  元数据表路径
     * @return 是否更新成功
     */
    public boolean updateTableMeta(String tableId, String columnName, String value, String tablePath) {
        try {
            logger.info("开始更新表元数据: {} 字段: {} 值: {}", tableId, columnName, 
                       value != null ? (value.length() > 100 ? value.substring(0, 100) + "..." : value) : "null");
            
            // 先查询现有记录
            Row[] existingRecord = (Row[]) queryTableMetaById(tableId, tablePath).collect();
            
            if (existingRecord.length == 0) {
                logger.error("✗ 未找到表ID为 {} 的记录", tableId);
                return false;
            }
            
            Row record = existingRecord[0];
            Timestamp currentTime = new Timestamp(System.currentTimeMillis());
            
            // 根据字段名更新对应的值
            MetaHudiTableRecord updatedRecord;
            
            switch (columnName.toLowerCase()) {
                case "schema":
                    updatedRecord = new MetaHudiTableRecord(
                        tableId,
                        value, // 更新schema字段
                        record.getAs(record.fieldIndex("status")),
                        record.getBoolean(record.fieldIndex("is_partitioned")),
                        record.getString(record.fieldIndex("partition_expr")),
                        record.getString(record.fieldIndex("hoodie_config")),
                        record.getString(record.fieldIndex("tags")),
                        record.getString(record.fieldIndex("description")),
                        record.getString(record.fieldIndex("source_db")),
                        record.getString(record.fieldIndex("source_table")),
                        record.getString(record.fieldIndex("db_type")),
                        record.getTimestamp(record.fieldIndex("create_time")),
                        currentTime,
                        record.getAs(record.fieldIndex("cdc_delete_flag"))
                    );
                    break;
                    
                case "partition_expr":
                    updatedRecord = new MetaHudiTableRecord(
                        tableId,
                        record.getString(record.fieldIndex("schema")),
                        record.getAs(record.fieldIndex("status")),
                        record.getBoolean(record.fieldIndex("is_partitioned")),
                        value, // 更新partition_expr字段
                        record.getString(record.fieldIndex("hoodie_config")),
                        record.getString(record.fieldIndex("tags")),
                        record.getString(record.fieldIndex("description")),
                        record.getString(record.fieldIndex("source_db")),
                        record.getString(record.fieldIndex("source_table")),
                        record.getString(record.fieldIndex("db_type")),
                        record.getTimestamp(record.fieldIndex("create_time")),
                        currentTime,
                        record.getAs(record.fieldIndex("cdc_delete_flag"))
                    );
                    break;
                    
                case "hoodie_config":
                    updatedRecord = new MetaHudiTableRecord(
                        tableId,
                        record.getString(record.fieldIndex("schema")),
                        record.getAs(record.fieldIndex("status")),
                        record.getBoolean(record.fieldIndex("is_partitioned")),
                        record.getString(record.fieldIndex("partition_expr")),
                        value, // 更新hoodie_config字段
                        record.getString(record.fieldIndex("tags")),
                        record.getString(record.fieldIndex("description")),
                        record.getString(record.fieldIndex("source_db")),
                        record.getString(record.fieldIndex("source_table")),
                        record.getString(record.fieldIndex("db_type")),
                        record.getTimestamp(record.fieldIndex("create_time")),
                        currentTime,
                        record.getAs(record.fieldIndex("cdc_delete_flag"))
                    );
                    break;
                    
                case "tags":
                    updatedRecord = new MetaHudiTableRecord(
                        tableId,
                        record.getString(record.fieldIndex("schema")),
                        record.getAs(record.fieldIndex("status")),
                        record.getBoolean(record.fieldIndex("is_partitioned")),
                        record.getString(record.fieldIndex("partition_expr")),
                        record.getString(record.fieldIndex("hoodie_config")),
                        value, // 更新tags字段
                        record.getString(record.fieldIndex("description")),
                        record.getString(record.fieldIndex("source_db")),
                        record.getString(record.fieldIndex("source_table")),
                        record.getString(record.fieldIndex("db_type")),
                        record.getTimestamp(record.fieldIndex("create_time")),
                        currentTime,
                        record.getAs(record.fieldIndex("cdc_delete_flag"))
                    );
                    break;
                    
                case "description":
                    updatedRecord = new MetaHudiTableRecord(
                        tableId,
                        record.getString(record.fieldIndex("schema")),
                        record.getAs(record.fieldIndex("status")),
                        record.getBoolean(record.fieldIndex("is_partitioned")),
                        record.getString(record.fieldIndex("partition_expr")),
                        record.getString(record.fieldIndex("hoodie_config")),
                        record.getString(record.fieldIndex("tags")),
                        value, // 更新description字段
                        record.getString(record.fieldIndex("source_db")),
                        record.getString(record.fieldIndex("source_table")),
                        record.getString(record.fieldIndex("db_type")),
                        record.getTimestamp(record.fieldIndex("create_time")),
                        currentTime,
                        record.getAs(record.fieldIndex("cdc_delete_flag"))
                    );
                    break;
                    
                case "source_db":
                    updatedRecord = new MetaHudiTableRecord(
                        tableId,
                        record.getString(record.fieldIndex("schema")),
                        record.getAs(record.fieldIndex("status")),
                        record.getBoolean(record.fieldIndex("is_partitioned")),
                        record.getString(record.fieldIndex("partition_expr")),
                        record.getString(record.fieldIndex("hoodie_config")),
                        record.getString(record.fieldIndex("tags")),
                        record.getString(record.fieldIndex("description")),
                        value, // 更新source_db字段
                        record.getString(record.fieldIndex("source_table")),
                        record.getString(record.fieldIndex("db_type")),
                        record.getTimestamp(record.fieldIndex("create_time")),
                        currentTime,
                        record.getAs(record.fieldIndex("cdc_delete_flag"))
                    );
                    break;
                    
                case "source_table":
                    updatedRecord = new MetaHudiTableRecord(
                        tableId,
                        record.getString(record.fieldIndex("schema")),
                        record.getAs(record.fieldIndex("status")),
                        record.getBoolean(record.fieldIndex("is_partitioned")),
                        record.getString(record.fieldIndex("partition_expr")),
                        record.getString(record.fieldIndex("hoodie_config")),
                        record.getString(record.fieldIndex("tags")),
                        record.getString(record.fieldIndex("description")),
                        record.getString(record.fieldIndex("source_db")),
                        value, // 更新source_table字段
                        record.getString(record.fieldIndex("db_type")),
                        record.getTimestamp(record.fieldIndex("create_time")),
                        currentTime,
                        record.getAs(record.fieldIndex("cdc_delete_flag"))
                    );
                    break;
                    
                case "db_type":
                    updatedRecord = new MetaHudiTableRecord(
                        tableId,
                        record.getString(record.fieldIndex("schema")),
                        record.getAs(record.fieldIndex("status")),
                        record.getBoolean(record.fieldIndex("is_partitioned")),
                        record.getString(record.fieldIndex("partition_expr")),
                        record.getString(record.fieldIndex("hoodie_config")),
                        record.getString(record.fieldIndex("tags")),
                        record.getString(record.fieldIndex("description")),
                        record.getString(record.fieldIndex("source_db")),
                        record.getString(record.fieldIndex("source_table")),
                        value, // 更新db_type字段
                        record.getTimestamp(record.fieldIndex("create_time")),
                        currentTime,
                        record.getAs(record.fieldIndex("cdc_delete_flag"))
                    );
                    break;
                    
                default:
                    logger.error("✗ 不支持的String类型字段: {}，支持的字段: schema, partition_expr, hoodie_config, tags, description, source_db, source_table, db_type", columnName);
                    return false;
            }
            
            // 创建DataFrame
            List<Row> rows = Collections.singletonList(createRowFromRecord(updatedRecord));
            StructType schema = createMetaTableSchema();
            Dataset<Row> updatedData = spark.createDataFrame(rows, schema);
            
            // 写入Hudi表进行更新（支持partial update）
            updatedData.write()
                      .format("hudi")
                      .mode("append")
                      .save(tablePath);
            
            logger.info("✓ 表元数据更新成功: {} 字段: {}", tableId, columnName);
            return true;
            
        } catch (Exception e) {
            logger.error("✗ 更新表元数据失败: {}", e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * 更新表元数据 - 支持更新boolean类型字段
     * 
     * @param tableId    表ID
     * @param columnName 列名（支持：is_partitioned）
     * @param value      boolean类型的值
     * @param tablePath  元数据表路径
     * @return 是否更新成功
     */
    public boolean updateTableMeta(String tableId, String columnName, boolean value, String tablePath) {
        try {
            logger.info("开始更新表元数据: {} 字段: {} 值: {}", tableId, columnName, value);
            
            // 先查询现有记录
            Row[] existingRecord = (Row[]) queryTableMetaById(tableId, tablePath).collect();
            
            if (existingRecord.length == 0) {
                logger.error("✗ 未找到表ID为 {} 的记录", tableId);
                return false;
            }
            
            Row record = existingRecord[0];
            Timestamp currentTime = new Timestamp(System.currentTimeMillis());
            
            // 根据字段名更新对应的值
            MetaHudiTableRecord updatedRecord;
            
            switch (columnName.toLowerCase()) {
                case "is_partitioned":
                    updatedRecord = new MetaHudiTableRecord(
                        tableId,
                        record.getString(record.fieldIndex("schema")),
                        record.getAs(record.fieldIndex("status")),
                        value, // 更新is_partitioned字段
                        record.getString(record.fieldIndex("partition_expr")),
                        record.getString(record.fieldIndex("hoodie_config")),
                        record.getString(record.fieldIndex("tags")),
                        record.getString(record.fieldIndex("description")),
                        record.getString(record.fieldIndex("source_db")),
                        record.getString(record.fieldIndex("source_table")),
                        record.getString(record.fieldIndex("db_type")),
                        record.getTimestamp(record.fieldIndex("create_time")),
                        currentTime,
                        record.getAs(record.fieldIndex("cdc_delete_flag"))
                    );
                    break;
                    
                default:
                    logger.error("✗ 不支持的boolean类型字段: {}，支持的字段: is_partitioned", columnName);
                    return false;
            }
            
            // 创建DataFrame
            List<Row> rows = Collections.singletonList(createRowFromRecord(updatedRecord));
            StructType schema = createMetaTableSchema();
            Dataset<Row> updatedData = spark.createDataFrame(rows, schema);
            
            // 写入Hudi表进行更新（支持partial update）
            updatedData.write()
                      .format("hudi")
                      .mode("append")
                      .save(tablePath);
            
            logger.info("✓ 表元数据更新成功: {} 字段: {} 值: {}", tableId, columnName, value);
            return true;
            
        } catch (Exception e) {
            logger.error("✗ 更新表元数据失败: {}", e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * 更新表元数据 - 通用方法，已过期，请使用类型安全的重载方法
     * 
     * @param tableId    表ID
     * @param columnName 列名
     * @param value      int类型的值
     * @return 是否更新成功
     * @deprecated 请使用 updateTableMeta(String tableId, String columnName, int value, String tablePath) 方法
     */
    @Deprecated
    public boolean updateTableMeta(String tableId, String columnName, int value) {
        logger.warn("使用了过期的updateTableMeta方法，请使用带tablePath参数的重载方法");
        return updateTableMeta(tableId, columnName, value, DEFAULT_META_TABLE_PATH);
    }
    
    /**
     * 检测Hudi表是否为分区表
     *
     * @param hudiTablePath Hudi表路径
     * @return 是否为分区表
     */
    private boolean detectPartitionedTable(String hudiTablePath) {
        try {
            // 读取Hudi表
            Dataset<Row> hudiTable = spark.read()
                                         .format("hudi")
                                         .load(hudiTablePath);
            
            // 检查是否包含分区相关的列
            String[] columns = hudiTable.columns();
            Set<String> columnSet = new HashSet<>(Arrays.asList(columns));
            
            boolean isPartitioned = columnSet.contains("cdc_dt") || 
                                   columnSet.stream().anyMatch(col -> col.toLowerCase().contains("cdc_dt"));
            
            logger.info("分区检测结果: {}", isPartitioned);
            return isPartitioned;
            
        } catch (Exception e) {
            logger.error("检测分区表失败，默认为非分区表: {}", e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * 从Hudi表路径中提取表名
     *
     * @param hudiTablePath Hudi表路径
     * @return 表名
     */
    private String extractTableNameFromPath(String hudiTablePath) {
        try {
            // 移除末尾的斜杠并获取最后一个路径段作为表名
            String cleanPath = hudiTablePath.replaceAll("/$", "");
            String[] pathSegments = cleanPath.split("/");
            String tableName = pathSegments[pathSegments.length - 1];
            
            // 如果表名为空，则使用倒数第二个路径段
            if (tableName.isEmpty() && pathSegments.length > 1) {
                tableName = pathSegments[pathSegments.length - 2];
            }
            
            return tableName.isEmpty() ? "table_" + System.currentTimeMillis() : tableName;
            
        } catch (Exception e) {
            logger.error("从路径提取表名失败，使用默认名称: {}", e.getMessage(), e);
            return "table_" + System.currentTimeMillis();
        }
    }
    
    /**
     * 打印表的DDL语句，用于调试和文档目的
     */
    public void printCreateTableDDL() {
        logger.info("=== meta_hudi_table 创建DDL ===");
        List<String> ddlStatements = getCreateHoodieMetaDDL();
        for (int i = 0; i < ddlStatements.size(); i++) {
            logger.info("--- 第 {} 个DDL语句 ---", i + 1);
            logger.info(ddlStatements.get(i));
            logger.info(""); // 空行分隔
        }
        logger.info("================================");
    }
    
    /**
     * 创建元数据表的Schema
     *
     * @return StructType
     */
    private StructType createMetaTableSchema() {
        return DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("id", DataTypes.StringType, false),
            DataTypes.createStructField("schema", DataTypes.StringType, false),
            DataTypes.createStructField("status", DataTypes.IntegerType, false),
            DataTypes.createStructField("is_partitioned", DataTypes.BooleanType, false),
            DataTypes.createStructField("partition_expr", DataTypes.StringType, true),
            DataTypes.createStructField("hoodie_config", DataTypes.StringType, true),
            DataTypes.createStructField("tags", DataTypes.StringType, true),
            DataTypes.createStructField("description", DataTypes.StringType, true),
            DataTypes.createStructField("source_db", DataTypes.StringType, true),
            DataTypes.createStructField("source_table", DataTypes.StringType, true),
            DataTypes.createStructField("db_type", DataTypes.StringType, true),
            DataTypes.createStructField("create_time", DataTypes.TimestampType, false),
            DataTypes.createStructField("update_time", DataTypes.TimestampType, false),
            DataTypes.createStructField("cdc_delete_flag", DataTypes.IntegerType, false)
        });
    }
    
    /**
     * 从MetaHudiTableRecord创建Row
     *
     * @param record 记录对象
     * @return Row
     */
    private Row createRowFromRecord(MetaHudiTableRecord record) {
        Object[] values = new Object[]{
            record.getId(),
            record.getSchema(),
            record.getStatus(),
            record.isPartitioned(),
            record.getPartitionExpr(),
            record.getHoodieConfig(),
            record.getTags(),
            record.getDescription(),
            record.getSourceDb(),
            record.getSourceTable(),
            record.getDbType(),
            record.getCreateTime(),
            record.getUpdateTime(),
            record.getCdcDeleteFlag()
        };
        
        return new GenericRowWithSchema(values, createMetaTableSchema());
    }
    
    /**
     * 工厂方法：创建MetaHudiTableManager实例
     *
     * @param spark SparkSession实例
     * @return MetaHudiTableManager实例
     */
    public static MetaHudiTableManager create(SparkSession spark) {
        return new MetaHudiTableManager(spark);
    }
} 