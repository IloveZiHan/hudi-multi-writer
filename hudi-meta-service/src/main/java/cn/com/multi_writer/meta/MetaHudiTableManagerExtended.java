package cn.com.multi_writer.meta;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;

import java.sql.*;
import java.util.*;
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * MetaHudiTableManager的扩展类，包含更多高级功能
 */
public class MetaHudiTableManagerExtended extends MetaHudiTableManager {
    
    private static final Logger logger = LoggerFactory.getLogger(MetaHudiTableManagerExtended.class);
    
    public MetaHudiTableManagerExtended(SparkSession spark) {
        super(spark);
    }
    
    /**
     * 基于TDSQL表的schema，自动创建并插入元数据记录到meta_hudi_table中
     *
     * @param tdsqlUrl        TDSQL数据库连接URL
     * @param tdsqlUser       TDSQL数据库用户名
     * @param tdsqlPassword   TDSQL数据库密码
     * @param tdsqlDatabase   TDSQL源数据库名称
     * @param tdsqlTable      TDSQL源表名称
     * @param tableId         在元数据表中的表标识符
     * @param status          表状态，默认为0（未上线）
     * @param partitionExpr   分区表达式
     * @param tags            表标签
     * @param description     表描述信息
     * @param metaTablePath   元数据表的存储路径
     * @return 是否操作成功
     */
    public boolean insertTableMetaFromTdsql(String tdsqlUrl, String tdsqlUser, String tdsqlPassword, 
                                           String tdsqlDatabase, String tdsqlTable, String tableId,
                                           int status, String partitionExpr, String tags, 
                                           String description, String metaTablePath) {
        Connection connection = null;
        try {
            logger.info("开始从TDSQL表获取schema并插入元数据表:");
            logger.info("  TDSQL URL: {}", tdsqlUrl);
            logger.info("  源数据库: {}", tdsqlDatabase);
            logger.info("  源表名: {}", tdsqlTable);
            
            // 获取HMS地址
            String hmsServerAddress = spark.conf().get("spark.hive.metastore.uris");
            
            // 创建数据库连接
            Properties properties = new Properties();
            properties.setProperty("user", tdsqlUser);
            properties.setProperty("password", tdsqlPassword);
            properties.setProperty("useSSL", "false");
            properties.setProperty("serverTimezone", "UTC");
            properties.setProperty("characterEncoding", "utf8");
            
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection(tdsqlUrl, properties);
            
            // 获取TDSQL表的schema
            Optional<StructType> tdsqlSchemaOpt = getTdsqlTableSchema(connection, tdsqlDatabase, tdsqlTable);
            
            if (!tdsqlSchemaOpt.isPresent()) {
                logger.error("✗ 无法获取TDSQL表schema: {}.{}", tdsqlDatabase, tdsqlTable);
                return false;
            }
            
            StructType tdsqlSchema = tdsqlSchemaOpt.get();
            logger.info("✓ 成功获取TDSQL表schema，字段数: {}", tdsqlSchema.fields().length);
            
            // 转换schema为JSON
            String schemaJson = tdsqlSchema.json();
            logger.info("生成的schema JSON: {}...", schemaJson.substring(0, Math.min(schemaJson.length(), 200)));
            
            // 使用表名作为ID（如果没有指定tableId）
            String finalTableId = tableId != null ? tableId : tdsqlDatabase + "_" + tdsqlTable;
            
            // 获取主键
            String primaryKey = getTdsqlTablePrimaryKey(connection, tdsqlDatabase, tdsqlTable);
            
            // 生成Hudi配置
            String hoodieConfig = generateHoodieConfig(finalTableId, primaryKey, hmsServerAddress);
            
            // 插入元数据记录
            boolean success = insertTableMeta(finalTableId, schemaJson, status, partitionExpr, 
                                            hoodieConfig, tags, description, tdsqlDatabase, 
                                            tdsqlTable, "TDSQL", metaTablePath);
            
            if (success) {
                logger.info("✓ 成功从TDSQL表创建元数据记录: {}", finalTableId);
                logger.info("  源库表: {}.{}", tdsqlDatabase, tdsqlTable);
                logger.info("  字段数: {}", tdsqlSchema.fields().length);
            } else {
                logger.error("✗ 从TDSQL表创建元数据记录失败: {}", finalTableId);
            }
            
            return success;
            
        } catch (Exception e) {
            logger.error("✗ 从TDSQL表插入元数据记录失败: {}", e.getMessage(), e);
            return false;
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    logger.error("关闭数据库连接失败: {}", e.getMessage());
                }
            }
        }
    }
    
    /**
     * 获取TDSQL表的主键字段
     *
     * @param connection TDSQL数据库连接
     * @param database   数据库名称
     * @param table      表名称
     * @return 主键字段名
     */
    private String getTdsqlTablePrimaryKey(Connection connection, String database, String table) throws SQLException {
        String sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
                    "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_KEY = 'PRI'";
        
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, database);
            statement.setString(2, table);
            
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getString("COLUMN_NAME").toLowerCase();
                }
            }
        }
        
        // 如果没有找到主键，返回默认的id字段
        return "id";
    }
    
    /**
     * 获取TDSQL表的schema信息
     *
     * @param connection TDSQL数据库连接
     * @param database   数据库名称
     * @param table      表名称
     * @return Optional<StructType> 表的schema
     */
    private Optional<StructType> getTdsqlTableSchema(Connection connection, String database, String table) {
        try {
            logger.info("连接TDSQL数据库获取表schema: {}.{}", database, table);
            
            // 查询表结构信息
            String sql = "SELECT " +
                        "    COLUMN_NAME, " +
                        "    DATA_TYPE, " +
                        "    CHARACTER_MAXIMUM_LENGTH, " +
                        "    NUMERIC_PRECISION, " +
                        "    NUMERIC_SCALE, " +
                        "    IS_NULLABLE, " +
                        "    COLUMN_DEFAULT, " +
                        "    COLUMN_COMMENT " +
                        "FROM INFORMATION_SCHEMA.COLUMNS " +
                        "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? " +
                        "ORDER BY ORDINAL_POSITION";
            
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, database);
                statement.setString(2, table);
                
                try (ResultSet resultSet = statement.executeQuery()) {
                    List<StructField> fields = new ArrayList<>();
                    
                    while (resultSet.next()) {
                        // 将字段名转换为小写
                        String columnName = resultSet.getString("COLUMN_NAME").toLowerCase();
                        String dataType = resultSet.getString("DATA_TYPE");
                        int maxLength = resultSet.getInt("CHARACTER_MAXIMUM_LENGTH");
                        int precision = resultSet.getInt("NUMERIC_PRECISION");
                        int scale = resultSet.getInt("NUMERIC_SCALE");
                        boolean isNullable = "YES".equalsIgnoreCase(resultSet.getString("IS_NULLABLE"));
                        String columnComment = resultSet.getString("COLUMN_COMMENT");
                        
                        // 映射TDSQL数据类型到Spark数据类型
                        DataType sparkDataType = mapTdsqlDataTypeToSpark(dataType, maxLength, precision, scale);
                        
                        // 创建StructField，添加注释信息作为metadata
                        Metadata metadata = Metadata.empty();
                        if (columnComment != null && !columnComment.trim().isEmpty()) {
                            metadata = new MetadataBuilder().putString("comment", columnComment).build();
                        }
                        
                        StructField field = DataTypes.createStructField(columnName, sparkDataType, isNullable, metadata);
                        fields.add(field);
                        
                        logger.info("  字段: {}, TDSQL类型: {} -> Spark类型: {}, 可空: {}", 
                                  columnName, dataType, sparkDataType, isNullable);
                    }
                    
                    if (!fields.isEmpty()) {
                        StructType schema = DataTypes.createStructType(fields);
                        logger.info("✓ 成功解析TDSQL表schema，共 {} 个字段", fields.size());
                        return Optional.of(schema);
                    } else {
                        logger.error("✗ 未找到表字段信息: {}.{}", database, table);
                        return Optional.empty();
                    }
                }
            }
            
        } catch (SQLException e) {
            logger.error("✗ 获取TDSQL表schema失败: {}", e.getMessage(), e);
            return Optional.empty();
        }
    }
    
    /**
     * 将TDSQL数据类型映射为Spark数据类型
     *
     * @param tdsqlDataType TDSQL数据类型
     * @param maxLength     最大长度
     * @param precision     精度
     * @param scale         小数位数
     * @return Spark DataType
     */
    private DataType mapTdsqlDataTypeToSpark(String tdsqlDataType, int maxLength, int precision, int scale) {
        String lowerType = tdsqlDataType.toLowerCase().trim();
        
        switch (lowerType) {
            // 整数类型
            case "tinyint":
                return precision == 1 ? DataTypes.BooleanType : DataTypes.ByteType;
            case "smallint":
                return DataTypes.ShortType;
            case "mediumint":
            case "int":
            case "integer":
                return DataTypes.IntegerType;
            case "bigint":
                return DataTypes.LongType;
            
            // 浮点数类型
            case "float":
                return DataTypes.FloatType;
            case "double":
            case "double precision":
                return DataTypes.DoubleType;
            case "real":
                return DataTypes.FloatType;
            
            // 定点数类型
            case "decimal":
            case "numeric":
                if (precision > 0 && scale >= 0) {
                    return DataTypes.createDecimalType(precision, scale);
                } else {
                    return DataTypes.createDecimalType(10, 0);
                }
            
            // 字符串类型
            case "char":
            case "varchar":
            case "tinytext":
            case "text":
            case "mediumtext":
            case "longtext":
            case "enum":
            case "set":
                return DataTypes.StringType;
            
            // 二进制类型
            case "binary":
            case "varbinary":
            case "tinyblob":
            case "blob":
            case "mediumblob":
            case "longblob":
                return DataTypes.BinaryType;
            
            // 日期时间类型
            case "date":
                return DataTypes.DateType;
            case "time":
                return DataTypes.StringType;
            case "datetime":
            case "timestamp":
                return DataTypes.TimestampType;
            case "year":
                return DataTypes.IntegerType;
            
            // JSON类型
            case "json":
                return DataTypes.StringType;
            
            // 几何类型
            case "geometry":
            case "point":
            case "linestring":
            case "polygon":
            case "multipoint":
            case "multilinestring":
            case "multipolygon":
            case "geometrycollection":
                return DataTypes.StringType;
            
            // 位类型
            case "bit":
                return precision == 1 ? DataTypes.BooleanType : DataTypes.BinaryType;
            
            // 其他未知类型默认为字符串
            default:
                logger.warn("警告: 未知的TDSQL数据类型 '{}'，映射为StringType", tdsqlDataType);
                return DataTypes.StringType;
        }
    }
    
    /**
     * 生成Hudi配置JSON字符串
     *
     * @param tableId           表ID
     * @param primaryKey        主键字段名
     * @param hmsServerAddress  HMS服务器地址
     * @return Hudi配置JSON字符串
     */
    private String generateHoodieConfig(String tableId, String primaryKey, String hmsServerAddress) {
        try {
            // 加载默认配置
            Map<String, String> defaultConfig = loadHoodieDefaultConfig();
            
            if (defaultConfig.isEmpty()) {
                logger.error("✗ 使用硬编码的默认配置");
                // 硬编码的默认配置
                Map<String, String> hardCodedConfig = new HashMap<>();
                hardCodedConfig.put("hoodie.table.name", tableId);
                hardCodedConfig.put("hoodie.datasource.write.table.type", "COPY_ON_WRITE");
                hardCodedConfig.put("hoodie.table.keygenerator.class", "org.apache.hudi.keygen.SimpleKeyGenerator");
                hardCodedConfig.put("hoodie.table.recordkey.fields", primaryKey);
                hardCodedConfig.put("hoodie.datasource.write.operation", "upsert");
                hardCodedConfig.put("hoodie.datasource.insert.dup.policy", "insert");
                hardCodedConfig.put("hoodie.datasource.meta.sync.enable", "true");
                hardCodedConfig.put("hoodie.datasource.meta_sync.condition.sync", "true");
                hardCodedConfig.put("hoodie.datasource.write.partitionpath.field", "cdc_dt");
                hardCodedConfig.put("hoodie.datasource.write.hive_style_partitioning", "true");
                hardCodedConfig.put("hoodie.datasource.hive_sync.enable", "true");
                hardCodedConfig.put("hoodie.datasource.hive_sync.mode", "hms");
                hardCodedConfig.put("hoodie.datasource.hive_sync.metastore.uris", hmsServerAddress);
                hardCodedConfig.put("hoodie.datasource.write.precombine.field", "update_time");
                hardCodedConfig.put("hoodie.index.type", "BUCKET");
                hardCodedConfig.put("hoodie.bucket.index.hash.field", "id");
                hardCodedConfig.put("hoodie.index.bucket.engine", "SIMPLE");
                hardCodedConfig.put("hoodie.bucket.index.num.buckets", "20");
                hardCodedConfig.put("hoodie.cleaner.commits.retained", "24");
                hardCodedConfig.put("hoodie.insert.shuffle.parallelism", "10");
                hardCodedConfig.put("hoodie.upsert.shuffle.parallelism", "10");
                hardCodedConfig.put("hoodie.bulkinsert.shuffle.parallelism", "500");
                
                return JSON.toJSONString(hardCodedConfig, SerializerFeature.DisableCircularReferenceDetect);
            }
            
            // 定义变量替换
            Map<String, String> variables = new HashMap<>();
            variables.put("primaryKey", primaryKey);
            variables.put("hmsServerAddress", hmsServerAddress);
            
            // 替换配置中的变量
            Map<String, String> finalConfig = replaceConfigVariables(defaultConfig, variables);
            
            // 转换为JSON字符串
            String jsonConfig = JSON.toJSONString(finalConfig, SerializerFeature.DisableCircularReferenceDetect);
            
            logger.info("✓ 成功生成Hudi配置，主键: {}", primaryKey);
            logger.info("  HMS地址: {}", hmsServerAddress);
            
            return jsonConfig;
            
        } catch (Exception e) {
            logger.error("✗ 生成Hudi配置失败: {}", e.getMessage(), e);
            return "{}";
        }
    }
    
    /**
     * 从resources目录加载Hudi默认配置
     *
     * @return 默认配置的Map
     */
    private Map<String, String> loadHoodieDefaultConfig() {
        try {
            // 从classpath加载配置文件
            InputStream configStream = getClass().getResourceAsStream("/hoodie-default.json");
            if (configStream != null) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(configStream, StandardCharsets.UTF_8))) {
                    StringBuilder configContent = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) {
                        configContent.append(line);
                    }
                    
                    // 解析JSON配置
                    JSONObject jsonObject = JSON.parseObject(configContent.toString());
                    Map<String, String> configMap = new HashMap<>();
                    
                    for (String key : jsonObject.keySet()) {
                        configMap.put(key, jsonObject.getString(key));
                    }
                    
                    logger.info("✓ 成功加载Hudi默认配置，配置项数量: {}", configMap.size());
                    return configMap;
                }
            } else {
                logger.error("✗ 无法找到hoodie-default.json配置文件");
                return new HashMap<>();
            }
        } catch (Exception e) {
            logger.error("✗ 加载Hudi默认配置失败: {}", e.getMessage(), e);
            return new HashMap<>();
        }
    }
    
    /**
     * 替换配置模板中的变量
     *
     * @param configMap 配置Map
     * @param variables 变量替换Map
     * @return 替换后的配置Map
     */
    private Map<String, String> replaceConfigVariables(Map<String, String> configMap, Map<String, String> variables) {
        Map<String, String> result = new HashMap<>();
        
        for (Map.Entry<String, String> entry : configMap.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            
            // 替换变量
            for (Map.Entry<String, String> varEntry : variables.entrySet()) {
                String varName = varEntry.getKey();
                String varValue = varEntry.getValue();
                value = value.replace("${" + varName + "}", varValue);
            }
            
            result.put(key, value);
        }
        
        return result;
    }
    
    /**
     * 根据table_id生成Hudi表的DDL语句
     *
     * @param tableId       表ID
     * @param tablePath     目标表的存储路径
     * @param metaTablePath 元数据表路径
     * @return DDL字符串，如果失败则返回null
     */
    public String generateHudiTableDDL(String tableId, String tablePath, String metaTablePath) {
        try {
            logger.info("开始为表 {} 生成Hudi表DDL...", tableId);
            
            // 1. 查询表的元数据信息
            Row[] metaRecord = (Row[]) queryTableMetaById(tableId, metaTablePath).collect();
            
            if (metaRecord.length == 0) {
                logger.error("✗ 未找到表ID为 {} 的元数据记录", tableId);
                return null;
            }
            
            Row record = metaRecord[0];
            String schemaJson = record.getString(record.fieldIndex("schema"));
            boolean isPartitioned = record.getBoolean(record.fieldIndex("is_partitioned"));
            String partitionExpr = record.getString(record.fieldIndex("partition_expr"));
            String hoodieConfig = record.getString(record.fieldIndex("hoodie_config"));
            String description = record.getString(record.fieldIndex("description"));
            String sourceDb = record.getString(record.fieldIndex("source_db"));
            String sourceTable = record.getString(record.fieldIndex("source_table"));
            String dbType = record.getString(record.fieldIndex("db_type"));
            
            logger.info("✓ 成功获取表元数据: {}", tableId);
            logger.info("  分区表: {}", isPartitioned);
            logger.info("  源库表: {}.{}", sourceDb != null ? sourceDb : "N/A", sourceTable != null ? sourceTable : "N/A");
            logger.info("  数据库类型: {}", dbType != null ? dbType : "N/A");
            
            // 2. 解析schema JSON为StructType
            StructType schema;
            try {
                schema = (StructType) DataType.fromJson(schemaJson);
            } catch (Exception e) {
                logger.error("✗ 解析schema JSON失败: {}", e.getMessage(), e);
                return null;
            }
            
            logger.info("✓ 成功解析schema，字段数: {}", schema.fields().length);
            
            // 3. 生成DDL语句
            String ddl = buildHudiTableDDL(tableId, schema, tablePath, isPartitioned, 
                                         partitionExpr, hoodieConfig, description);
            
            logger.info("✓ 成功生成Hudi表DDL: {}", tableId);
            return ddl;
            
        } catch (Exception e) {
            logger.error("✗ 生成Hudi表DDL失败: {}", e.getMessage(), e);
            return null;
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
    private String buildHudiTableDDL(String tableId, StructType schema, String tablePath, 
                                    boolean isPartitioned, String partitionExpr, 
                                    String hoodieConfig, String description) {
        
        // 1. 构建字段定义
        List<String> fieldDefs = new ArrayList<>();
        for (StructField field : schema.fields()) {
            String fieldName = field.name();
            String dataType = sparkDataTypeToSqlString(field.dataType());
            String nullable = field.nullable() ? "" : " NOT NULL";
            
            String comment = "";
            if (field.metadata() != null && field.metadata().contains("comment")) {
                String commentStr = field.metadata().getString("comment");
                if (commentStr != null && !commentStr.trim().isEmpty()) {
                    comment = String.format(" COMMENT '%s'", commentStr);
                }
            }
            
            fieldDefs.add(String.format("    %s %s%s%s", fieldName, dataType, nullable, comment));
        }
        
        String fieldDefinitions = String.join(",\n", fieldDefs);
        
        // 2. 构建表注释
        String tableComment = "";
        if (description != null && !description.trim().isEmpty()) {
            tableComment = String.format("\nCOMMENT '%s'", description);
        }
        
        // 3. 构建分区定义
        String partitionClause = "";
        if (isPartitioned) {
            partitionClause = "\nPARTITIONED BY (cdc_dt String)";
        }
        
        // 4. 构建TBLPROPERTIES
        String tblProperties = buildTblProperties(tableId, hoodieConfig);
        
        // 5. 构建完整的DDL
        return String.format(
            "CREATE TABLE IF NOT EXISTS %s (\n" +
            "%s\n" +
            ") USING HUDI%s\n" +
            "LOCATION '%s'%s\n" +
            "%s",
            tableId, fieldDefinitions, tableComment, tablePath, partitionClause, tblProperties
        );
    }
    
    /**
     * 将Spark DataType转换为SQL字符串表示
     *
     * @param dataType Spark DataType
     * @return SQL数据类型字符串
     */
    private String sparkDataTypeToSqlString(DataType dataType) {
        if (dataType instanceof StringType) {
            return "STRING";
        } else if (dataType instanceof IntegerType) {
            return "INT";
        } else if (dataType instanceof LongType) {
            return "BIGINT";
        } else if (dataType instanceof DoubleType) {
            return "DOUBLE";
        } else if (dataType instanceof FloatType) {
            return "FLOAT";
        } else if (dataType instanceof BooleanType) {
            return "BOOLEAN";
        } else if (dataType instanceof TimestampType) {
            return "TIMESTAMP";
        } else if (dataType instanceof DateType) {
            return "DATE";
        } else if (dataType instanceof BinaryType) {
            return "BINARY";
        } else if (dataType instanceof ByteType) {
            return "TINYINT";
        } else if (dataType instanceof ShortType) {
            return "SMALLINT";
        } else if (dataType instanceof DecimalType) {
            DecimalType dt = (DecimalType) dataType;
            return String.format("DECIMAL(%d,%d)", dt.precision(), dt.scale());
        } else {
            return dataType.sql();
        }
    }
    
    /**
     * 构建TBLPROPERTIES配置
     *
     * @param tableId      表ID
     * @param hoodieConfig Hudi配置JSON
     * @return TBLPROPERTIES字符串
     */
    private String buildTblProperties(String tableId, String hoodieConfig) {
        Map<String, String> allProperties = new HashMap<>();
        
        // 判断hoodieConfig是否为空
        if (hoodieConfig != null && !hoodieConfig.trim().isEmpty()) {
            try {
                JSONObject jsonObject = JSON.parseObject(hoodieConfig);
                for (String key : jsonObject.keySet()) {
                    allProperties.put(key, jsonObject.getString(key));
                }
            } catch (Exception e) {
                logger.error("解析Hudi配置失败: {}", e.getMessage());
            }
        }
        
        // 构建TBLPROPERTIES字符串
        List<String> properties = new ArrayList<>();
        for (Map.Entry<String, String> entry : allProperties.entrySet()) {
            properties.add(String.format("    '%s' = '%s'", entry.getKey(), entry.getValue()));
        }
        
        return String.format("TBLPROPERTIES (\n%s\n)", String.join(",\n", properties));
    }
    
    /**
     * 生成并打印Hudi表的DDL语句
     *
     * @param tableId       表ID
     * @param tablePath     目标表的存储路径
     * @param metaTablePath 元数据表路径
     * @return DDL语句字符串，失败时返回空字符串
     */
    public String generateAndPrintHudiTableDDL(String tableId, String tablePath, String metaTablePath) {
        try {
            String ddl = generateHudiTableDDL(tableId, tablePath, metaTablePath);
            
            if (ddl != null) {
                logger.info("\n=== Hudi表 {} 的DDL语句 ===", tableId);
                logger.info(ddl);
                logger.info("==================================================");
                return ddl;
            } else {
                logger.error("✗ 无法生成表 {} 的DDL语句", tableId);
                return "";
            }
            
        } catch (Exception e) {
            logger.error("✗ 生成并打印DDL失败: {}", e.getMessage(), e);
            return "";
        }
    }
    
    /**
     * 工厂方法：创建MetaHudiTableManagerExtended实例
     *
     * @param spark SparkSession实例
     * @return MetaHudiTableManagerExtended实例
     */
    public static MetaHudiTableManagerExtended createExtended(SparkSession spark) {
        return new MetaHudiTableManagerExtended(spark);
    }
} 