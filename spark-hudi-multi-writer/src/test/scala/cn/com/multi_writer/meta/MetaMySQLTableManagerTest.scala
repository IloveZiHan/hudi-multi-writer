package cn.com.multi_writer.meta

import cn.com.multi_writer.SparkSessionManager
import org.apache.spark.sql.SparkSession
import org.junit.{Before, Test}


/**
 * MetaMySQLTableManager的测试用例
 *
 * 使用嵌入式H2数据库模拟MySQL数据库进行测试
 */
class MetaMySQLTableManagerTest {

    private var spark: SparkSession = _
    private var metaManager: MetaTableManager = _

    @Before
    def beforeAll(): Unit = {
        spark = SparkSessionManager.createSparkSession("local")
        val configs = SparkSessionManager.getStreamingConfigs(spark)

        metaManager = MetaTableManagerFactory.createMySQLMetaTableManager(spark, configs("mysqlUrl"), configs("mysqlUser"), configs("mysqlPassword"))
    }

    @Test
    def testQueryTablesByApplicationName01(): Unit = {
        val result = metaManager.queryTablesByApplicationName("hudi-cdc-stream-job")
        result.show()
    }
} 