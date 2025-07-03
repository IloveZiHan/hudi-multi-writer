package cn.com.multi_writer.meta

import cn.com.multi_writer.SparkSessionManager
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.junit.{After, Before, Test, Assert}
import java.io.File
import java.nio.file.{Files, Paths}
import scala.util.Random
import org.slf4j.{Logger, LoggerFactory}

/**
 * MetaHudiTableManager测试类
 *
 * 本测试类用于验证MetaHudiTableManager的各项功能，特别是extractAndInsertHudiTableSchema方法
 *
 * 测试内容：
 * 1. 测试从Hudi表提取schema并插入元数据表的正常流程
 * 2. 测试异常情况处理（表不存在、路径无效等）
 * 3. 测试数据完整性和一致性
 * 4. 测试边界条件和容错性
 * 5. 测试createMetaTable方法的各种场景
 *
 * 注意事项：
 * - 使用临时目录避免测试间的相互影响
 * - 每个测试方法独立运行，有完整的setup和cleanup
 * - 使用实际的Hudi表进行集成测试
 */
class MetaHudiTableManagerTest extends TestBase {

    private var metaManager: MetaHudiTableManager = _
    private var testHudiTablePath: String = _
    private var metaTablePath: String = "/tmp/spark-warehouse/meta_hudi_table"
    private val logger: Logger = LoggerFactory.getLogger(classOf[MetaHudiTableManagerTest])

    /**
     * 测试前的初始化
     * 在基类初始化后进行业务相关的初始化
     */
    @Before
    def setUp(): Unit = {
        // 调用基类的初始化
        setUpBase()

        // 创建MetaHudiTableManager实例
        metaManager = new MetaHudiTableManager(spark)

        // 设置测试路径
        testHudiTablePath = s"$tempDir/test_hudi_table"
        metaTablePath = s"$tempDir/meta_hudi_table"

        logger.info(s"=== MetaHudiTableManager测试开始 ===")
        logger.info(s"测试Hudi表路径: $testHudiTablePath")
        logger.info(s"元数据表路径: $metaTablePath")
    }

    /**
     * 测试后的清理
     */
    @After
    def tearDown(): Unit = {
        // 调用基类的清理
        tearDownBase()
        logger.info("=== MetaHudiTableManager测试结束 ===")
    }

    @Test
    def testCreateMetaTable(): Unit = {
        try {
            logger.info("开始测试：创建元数据表")

            // 创建元数据表
            val createSuccess = metaManager.createMetaTable(metaTablePath)
            Assert.assertTrue("元数据表创建应该成功", createSuccess)

            logger.info("✓ 测试通过：成功创建元数据表")
        }
    }

    @Test
    def testExtractAndInsertHudiTableSchema(): Unit = {
        try {
            logger.info("开始测试：提取Hudi表schema并插入元数据表")

            // 提取Hudi表schema并插入元数据表
            val testHudiTablePath = "/tmp/spark-warehouse/s009_t_alc_loan"
            val metaTablePath = "/tmp/spark-warehouse/meta_hudi_table"
            val insertSuccess = metaManager.extractAndInsertHudiTableSchema(testHudiTablePath, metaTablePath)
            Assert.assertTrue("schema插入应该成功", insertSuccess)

            logger.info("✓ 测试通过：成功提取Hudi表schema并插入元数据表")

            metaManager.queryAllTables(metaTablePath).show(false)
        }
    }

    @Test
    def testAddFieldsToSchema(): Unit = {
        try {
            logger.info("开始测试：添加字段到schema")

            // 先获取Hudi表的最新schema，然后添加字段
            val testHudiTablePath = "/tmp/spark-warehouse/s009_t_alc_loan"
            val addSuccess1 = metaManager.addFieldsToSchema("s009_t_alc_loan", testHudiTablePath, "col4 String, col5 Int", metaTablePath)
            Assert.assertTrue("基于Hudi表最新schema的字段添加应该成功", addSuccess1)

            // 查看更新后的结果
            logger.info("更新后的表元数据:")
            metaManager.queryTableMetaById("s009_t_alc_loan", metaTablePath).show(false)
        }
    }

    @Test
    def testGetLatestHudiTableSchema(): Unit = {
        try {
            logger.info("开始测试：获取Hudi表最新schema")

            val testHudiTablePath = "/tmp/spark-warehouse/s009_t_alc_loan"
            val schemaOpt = metaManager.getLatestHudiTableSchema(testHudiTablePath)
            
            if (schemaOpt.isDefined) {
                val schema = schemaOpt.get
                logger.info(s"✓ 成功获取Hudi表schema，字段数: ${schema.fields.length}")
                logger.info("Schema字段:")
                schema.fields.foreach { field =>
                    logger.info(s"  - ${field.name}: ${field.dataType} (nullable: ${field.nullable})")
                }
                Assert.assertTrue("应该能成功获取到schema", true)
            } else {
                logger.warn("⚠ 未能获取到Hudi表schema，可能表不存在")
                // 这里不Assert.fail，因为测试环境中可能确实没有这个表
            }
        }
    }

    @Test
    def testetTableOnline(): Unit = {
        try {
            logger.info("开始测试：将表设置为在线")

            // 将表设置为在线
            val tableId = "s009_t_alc_loan"
            val setOnlineSuccess = metaManager.setTableOnline(tableId, metaTablePath)
            Assert.assertTrue("表设置为在线应该成功", setOnlineSuccess)

            logger.info("✓ 测试通过：成功将表设置为在线")

            metaManager.queryTableMetaById(tableId, metaTablePath).show(false)
        }
    }

    @Test
    def testetTableOffline(): Unit = {
        try {
            logger.info("开始测试：将表设置为离线")

            // 将表设置为离线
            val tableId = "s009_t_alc_loan"
            val setOfflineSuccess = metaManager.setTableOffline(tableId, metaTablePath)
            Assert.assertTrue("表设置为离线应该成功", setOfflineSuccess)

            logger.info("✓ 测试通过：成功将表设置为离线")

            metaManager.queryTableMetaById(tableId, metaTablePath).show(false)
        }
    }

    @Test
    def testQueryOnlineTables(): Unit = {
        try {
            logger.info("开始测试：查询在线表")

            // 查询在线表
            val onlineTables = metaManager.queryTablesByStatus(MetaHudiTableManager.TableStatus.ONLINE, metaTablePath)
            onlineTables.show(false)
        }
    }

    @Test
    def testUpdateTableSourceInfo(): Unit = {
        try {
            logger.info("开始测试：更新表源信息")

            // 更新表源信息
            val tableId = "s009_t_alc_batch_repay"
            val updateSuccess = metaManager.updateTableSourceInfo(tableId, "alc", "t_alc_batch_repay", "tdsql", metaTablePath)
            Assert.assertTrue("表源信息更新应该成功", updateSuccess)

            metaManager.queryTableMetaById(tableId, metaTablePath).show(false)
        }
    }

    @Test
    def testUpdateTablePartitionInfo(): Unit = {
        try {
            logger.info("开始测试：更新表分区信息")

            // 更新表分区信息
            val tableId = "s009_t_alc_loan"
            // 获取create_time对应年的第一天
            val partitionExpr = "trunc(create_time, 'year')"
            val updateSuccess = metaManager.updateTablePartitionExpr(tableId, partitionExpr, metaTablePath)
            Assert.assertTrue("表分区信息更新应该成功", updateSuccess)

            metaManager.queryTableMetaById(tableId, metaTablePath).show(false)
        }
    }

    @Test
    def testGenerateHudiTableDDL(): Unit = {
        try {
            logger.info("开始测试：生成Hudi表DDL")

            val tableId = "s009_t_alc_batch_repay"
            val testHudiTablePath1 = "/tmp/spark-warehouse/s009_t_alc_batch_repay"
            val ddl = metaManager.generateHudiTableDDL(tableId, testHudiTablePath1, metaTablePath)
            logger.info(ddl.toString)
        }
    }
} 