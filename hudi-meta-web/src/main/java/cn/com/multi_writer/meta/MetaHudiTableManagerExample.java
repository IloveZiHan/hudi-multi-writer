package cn.com.multi_writer.meta;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MetaHudiTableManager Java版本使用示例
 * 
 * 展示如何使用Java版本的Hudi表元数据管理器
 */
public class MetaHudiTableManagerExample {
    
    private static final Logger logger = LoggerFactory.getLogger(MetaHudiTableManagerExample.class);
    
    public static void main(String[] args) {
        // 1. 创建SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("MetaHudiTableManagerExample")
                .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
                .config("spark.hive.metastore.uris", "thrift://localhost:9083")
                .enableHiveSupport()
                .getOrCreate();
        
        try {
            // 2. 创建MetaHudiTableManager实例
            MetaHudiTableManager metaManager = new MetaHudiTableManager(spark);
            MetaHudiTableManagerExtended extendedManager = new MetaHudiTableManagerExtended(spark);
            
            // 3. 定义路径
            String metaTablePath = "/tmp/hudi_tables/meta_hudi_table";
            String testTablePath = "/tmp/hudi_tables/test_table";
            
            // 4. 创建元数据表
            logger.info("=== 创建元数据表 ===");
            boolean createResult = metaManager.createMetaTable();
            if (createResult) {
                logger.info("✓ 元数据表创建成功");
            } else {
                logger.error("✗ 元数据表创建失败");
                return;
            }
            
            // 5. 插入测试数据
            logger.info("\n=== 插入测试表元数据 ===");
            String schemaJson = "{\n" +
                "  \"type\": \"struct\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"id\", \"type\": \"string\", \"nullable\": false, \"metadata\": {}},\n" +
                "    {\"name\": \"name\", \"type\": \"string\", \"nullable\": true, \"metadata\": {}},\n" +
                "    {\"name\": \"age\", \"type\": \"integer\", \"nullable\": true, \"metadata\": {}},\n" +
                "    {\"name\": \"create_time\", \"type\": \"timestamp\", \"nullable\": true, \"metadata\": {}}\n" +
                "  ]\n" +
                "}";
            
            boolean insertResult = metaManager.insertTableMeta(
                "test_table_001",           // tableId
                schemaJson,                 // schemaJson
                MetaHudiTableManager.TableStatus.OFFLINE,  // status
                "trunc(create_time, 'year')",  // partitionExpr
                null,                       // hoodieConfig
                "测试表,示例表",             // tags
                "这是一个测试表",           // description
                "test_db",                  // sourceDb
                "users",                    // sourceTable
                "MySQL",                    // dbType
                metaTablePath               // tablePath
            );
            
            if (insertResult) {
                logger.info("✓ 测试表元数据插入成功");
            } else {
                logger.error("✗ 测试表元数据插入失败");
                return;
            }
            
            // 6. 查询所有表
            logger.info("\n=== 查询所有表 ===");
            Dataset<Row> allTables = metaManager.queryAllTables(metaTablePath);
            logger.info("总表数量: {}", allTables.count());
            allTables.show(false);
            
            // 7. 根据ID查询特定表
            logger.info("\n=== 根据ID查询特定表 ===");
            Dataset<Row> specificTable = metaManager.queryTableMetaById("test_table_001", metaTablePath);
            specificTable.show(false);
            
            // 8. 更新表状态为上线
            logger.info("\n=== 更新表状态为上线 ===");
            boolean updateResult = metaManager.setTableOnline("test_table_001", metaTablePath);
            if (updateResult) {
                logger.info("✓ 表状态更新成功");
            } else {
                logger.error("✗ 表状态更新失败");
            }
            
            // 9. 查询已上线的表
            logger.info("\n=== 查询已上线的表 ===");
            Dataset<Row> onlineTables = metaManager.queryOnlineTables(metaTablePath);
            logger.info("已上线表数量: {}", onlineTables.count());
            onlineTables.show(false);
            
            // 10. 根据数据库类型查询表
            logger.info("\n=== 根据数据库类型查询表 ===");
            Dataset<Row> mysqlTables = metaManager.queryTablesByDbType("MySQL", metaTablePath);
            logger.info("MySQL表数量: {}", mysqlTables.count());
            mysqlTables.show(false);
            
            // 11. 根据标签查询表
            logger.info("\n=== 根据标签查询表 ===");
            Dataset<Row> testTables = metaManager.queryTablesByTag("测试表", metaTablePath);
            logger.info("测试表数量: {}", testTables.count());
            testTables.show(false);
            
            // 12. 生成DDL语句
            logger.info("\n=== 生成DDL语句 ===");
            String ddl = extendedManager.generateHudiTableDDL("test_table_001", testTablePath, metaTablePath);
            if (ddl != null) {
                logger.info("生成的DDL语句:\n{}", ddl);
            } else {
                logger.error("✗ DDL生成失败");
            }
            
            // 13. 从TDSQL插入表元数据（注释掉，因为需要实际的TDSQL连接）
            /*
            logger.info("\n=== 从TDSQL插入表元数据 ===");
            boolean tdsqlResult = extendedManager.insertTableMetaFromTdsql(
                "jdbc:mysql://localhost:3306/test_db",  // tdsqlUrl
                "root",                                 // tdsqlUser
                "password",                             // tdsqlPassword
                "test_db",                              // tdsqlDatabase
                "users",                                // tdsqlTable
                "tdsql_users_table",                    // tableId
                MetaHudiTableManager.TableStatus.OFFLINE,  // status
                "trunc(create_time, 'year')",          // partitionExpr
                "TDSQL表,用户表",                       // tags
                "从TDSQL同步的用户表",                   // description
                metaTablePath                           // metaTablePath
            );
            
            if (tdsqlResult) {
                logger.info("✓ 从TDSQL插入表元数据成功");
            } else {
                logger.error("✗ 从TDSQL插入表元数据失败");
            }
            */
            
            // 14. 打印创建表的DDL
            logger.info("\n=== 打印创建表的DDL ===");
            metaManager.printCreateTableDDL();
            
            logger.info("\n=== 示例执行完成 ===");
            
        } catch (Exception e) {
            logger.error("执行示例时发生错误: {}", e.getMessage(), e);
        } finally {
            // 关闭SparkSession
            spark.stop();
        }
    }
    
    /**
     * 演示基本的CRUD操作
     */
    public static void demonstrateBasicOperations(SparkSession spark) {
        logger.info("=== 演示基本的CRUD操作 ===");
        
        MetaHudiTableManager metaManager = new MetaHudiTableManager(spark);
        String metaTablePath = "/tmp/hudi_tables/meta_hudi_table_demo";
        
        try {
            // 1. 创建元数据表
            metaManager.createMetaTable();
            
            // 2. 插入多个测试表
            String[] tableIds = {"demo_table_001", "demo_table_002", "demo_table_003"};
            String[] sourceTables = {"orders", "products", "customers"};
            String[] descriptions = {"订单表", "产品表", "客户表"};
            
            for (int i = 0; i < tableIds.length; i++) {
                String simpleSchema = String.format(
                    "{\n" +
                    "  \"type\": \"struct\",\n" +
                    "  \"fields\": [\n" +
                    "    {\"name\": \"id\", \"type\": \"string\", \"nullable\": false, \"metadata\": {}},\n" +
                    "    {\"name\": \"name\", \"type\": \"string\", \"nullable\": true, \"metadata\": {}},\n" +
                    "    {\"name\": \"update_time\", \"type\": \"timestamp\", \"nullable\": true, \"metadata\": {}}\n" +
                    "  ]\n" +
                    "}"
                );
                
                metaManager.insertTableMeta(
                    tableIds[i],
                    simpleSchema,
                    i % 2, // 交替设置上线/下线状态
                    null,
                    null,
                    "演示表",
                    descriptions[i],
                    "demo_db",
                    sourceTables[i],
                    "MySQL",
                    metaTablePath
                );
                
                logger.info("✓ 插入表: {}", tableIds[i]);
            }
            
            // 3. 查询所有表
            Dataset<Row> allTables = metaManager.queryAllTables(metaTablePath);
            logger.info("总表数量: {}", allTables.count());
            
            // 4. 查询已上线的表
            Dataset<Row> onlineTables = metaManager.queryOnlineTables(metaTablePath);
            logger.info("已上线表数量: {}", onlineTables.count());
            
            // 5. 根据源数据库查询表
            Dataset<Row> demoDbTables = metaManager.queryTablesBySourceDb("demo_db", metaTablePath);
            logger.info("demo_db表数量: {}", demoDbTables.count());
            
            // 6. 更新表状态
            metaManager.setTableOnline("demo_table_001", metaTablePath);
            logger.info("✓ 表demo_table_001状态更新为上线");
            
            // 7. 再次查询已上线的表
            onlineTables = metaManager.queryOnlineTables(metaTablePath);
            logger.info("更新后已上线表数量: {}", onlineTables.count());
            
            logger.info("=== 基本操作演示完成 ===");
            
        } catch (Exception e) {
            logger.error("演示基本操作时发生错误: {}", e.getMessage(), e);
        }
    }
} 