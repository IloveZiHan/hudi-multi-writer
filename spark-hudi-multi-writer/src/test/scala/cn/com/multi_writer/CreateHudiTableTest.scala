package cn.hnzycfc.hudi

import cn.com.multi_writer.SparkSessionManager
import org.apache.spark.sql.SparkSession
import org.junit.{After, Before, Test}

/**
 * Hudi表创建测试类
 * 
 * 本测试类用于验证通过SparkSession执行DDL语句创建Hudi表的功能
 * 
 * 测试内容：
 * 1. 删除已存在的表（如果存在）
 * 2. 创建包含完整字段定义的Hudi表
 * 3. 验证表创建成功
 * 
 * 注意事项：
 * - 使用SparkSessionManager统一管理SparkSession
 * - 按照Hudi 0.15版本的最佳实践配置表属性
 * - 使用BUCKET索引和COPY_ON_WRITE表类型以获得最佳性能
 */
class CreateHudiTableTest {

  private var spark: SparkSession = _

  /**
   * 测试前的初始化
   * 创建SparkSession实例
   */
  @Before
  def setUp(): Unit = {
    // 使用SparkSessionManager创建SparkSession
    spark = SparkSessionManager.createSparkSession("Create Hudi Table Test")
    
    // 设置日志级别为WARN，减少日志输出
    spark.sparkContext.setLogLevel("WARN")
    
    println("=== Hudi表创建测试开始 ===")
  }

  /**
   * 测试后的清理
   * 停止SparkSession
   */
  @After
  def tearDown(): Unit = {
    if (spark != null && !spark.sparkContext.isStopped) {
      spark.stop()
    }
    println("=== Hudi表创建测试结束 ===")
  }

  /**
   * 测试创建Hudi表
   * 
   * 执行以下操作：
   * 1. 删除已存在的表（如果存在）
   * 2. 创建新的Hudi表
   * 3. 验证表创建成功
   */
  @Test
  def testCreateHudiTable(): Unit = {
    try {
      println("开始执行Hudi表创建DDL...")
      
      // 1. 删除已存在的表（如果存在）
      println("1. 删除已存在的表（如果存在）...")
      val dropTableSql = "DROP TABLE IF EXISTS s009_t_alc_loan_bucket"
      spark.sql(dropTableSql).collect()
      println("✓ 表删除完成")
      
      // 2. 创建Hudi表
      println("2. 创建Hudi表...")
      val createTableSql = """
        |CREATE TABLE IF NOT EXISTS s009_t_alc_loan_bucket (
        |    id varchar(64) COMMENT '物理主键',
        |    loan_no varchar(120) COMMENT '借据号',
        |    cont_no varchar(64) COMMENT '合同号',
        |    cust_no varchar(64) COMMENT '客户编号',
        |    cust_name varchar(120) COMMENT '客户姓名',
        |    id_type varchar(40) COMMENT '证件类型',
        |    id_no varchar(36) COMMENT '证件号码',
        |    prod_no varchar(64) COMMENT '产品编号',
        |    prod_id varchar(64) COMMENT '产品ID',
        |    trade_dt varchar(40) COMMENT '交易日期',
        |    loan_sts varchar(40) COMMENT '借据状态',
        |    int_start_dt varchar(40) COMMENT '起息日',
        |    loan_term int COMMENT '贷款期限',
        |    due_dt varchar(40) COMMENT '到期日',
        |    repay_dt int COMMENT '还款日',
        |    repay_intrv_unit varchar(40) COMMENT '还款间隔单位',
        |    repay_intrv int COMMENT '还款间隔',
        |    repay_mode_code varchar(40) COMMENT '还款方式代码',
        |    instm_ind varchar(40) COMMENT '期供标志',
        |    loan_tnr int COMMENT '贷款期数',
        |    loan_account_sts varchar(40) COMMENT '借据账务状态',
        |    fst_repay_dt varchar(40) COMMENT '第一次扣款日',
        |    last_repay_dt varchar(40) COMMENT '上次还款日',
        |    curr_repay_dt varchar(40) COMMENT '当前还款日',
        |    next_repay_dt varchar(40) COMMENT '下一还款日',
        |    five_lvl_class_code varchar(40) COMMENT '五级分类编码',
        |    int_rate decimal(16,10) COMMENT '执行利率',
        |    od_int_rate decimal(16,10) COMMENT '罚息利率',
        |    loan_amt decimal(16,2) COMMENT '贷款金额',
        |    loan_bal decimal(16,2) COMMENT '贷款余额',
        |    wrtoff_dt varchar(40) COMMENT '核销日期',
        |    grace_perd_type varchar(40) COMMENT '宽限期类型',
        |    grace_perd_days int COMMENT '宽限期天数',
        |    calc_od_int_part varchar(40) COMMENT '计算罚息部分',
        |    loan_deflt_ind varchar(40) COMMENT '贷款拖欠标志',
        |    last_provi_dt varchar(20) COMMENT '上次计提日',
        |    last_tran_over_dte varchar(40) COMMENT '上次转逾期日',
        |    last_deduct_dt varchar(40) COMMENT '上一次扣款日',
        |    curr_jon_penal_fee_dt varchar(40) COMMENT '当前结违约金日期',
        |    curr_jon_od_int_dt varchar(40) COMMENT '当前结罚息日期',
        |    last_provi_od_int_dt varchar(40) COMMENT '上次计提罚息日期',
        |    loan_deval_ind varchar(40) COMMENT '贷款减值标志',
        |    loan_deval_dt varchar(40) COMMENT '贷款减值日期',
        |    tran_off_bs_ind varchar(40) COMMENT '转表外标志',
        |    tran_off_bs_dt varchar(40) COMMENT '转表外日期',
        |    ori_due_dt varchar(40) COMMENT '原到期日',
        |    vat_tax_rate decimal(16,9) COMMENT '增值税税率',
        |    loan_lock_sts varchar(40) COMMENT '借据锁定状态',
        |    deval_repay_seq varchar(40) COMMENT '减值还款顺序',
        |    coopr_cont_no varchar(64) COMMENT '项目协议编号',
        |    agent_repay_ind varchar(40) COMMENT '代还款标志',
        |    delay_pay_ind varchar(40) COMMENT '延迟支付标志',
        |    coopr_biz_seq varchar(128) COMMENT '合作方业务流水',
        |    chan_no varchar(40) COMMENT '进件渠道',
        |    prft_type varchar(10) COMMENT '收益类型',
        |    day_rate_calc_base varchar(40) COMMENT '日利率天数计算基础360/365',
        |    last_sub varchar(40) COMMENT '尾期是否补差: Y-是N-否',
        |    account_org varchar(64) COMMENT '账务机构',
        |    create_time varchar(45) COMMENT '创建时间',
        |    create_user varchar(64) COMMENT '创建人',
        |    update_time varchar(45) COMMENT '修改时间',
        |    update_user varchar(64) COMMENT '修改人',
        |    del_flag varchar(2) COMMENT '逻辑删标志',
        |    risk_five_lvl_class_code varchar(40) COMMENT '风险认定五级分类编码',
        |    unite_crdt_ind varchar(40) COMMENT '联合贷标志 Y-是,N-否',
        |    our_pty_contri_ratio decimal(16,10) COMMENT '我方出资比例',
        |    insure_ind varchar(40) COMMENT '承保标志',
        |    loan_bal_accum decimal(16,2) COMMENT '借据余额积数',
        |    portfolio_code varchar(64) COMMENT '资产池编号',
        |    confirmation_type varchar(4) COMMENT '终止确认类型Y出表N不出表',
        |    src_sysname varchar(10) COMMENT '来源源系统代码',
        |    src_table varchar(30) COMMENT '来源源系统表名',
        |    etl_tx_dt varchar(10) COMMENT 'ETL业务日期',
        |    etl_first_dt varchar(10) COMMENT 'ETL首次插入日期',
        |    etl_proc_dt varchar(50) COMMENT 'ETL处理时间戳',
        |    start_dt varchar(10) COMMENT '开始日期',
        |    fund_type varchar(20) COMMENT '放款类型',
        |    amort_loan_no varchar(120) COMMENT '账单分期借据号',
        |    comb_rate decimal(16,10) COMMENT '综合年化利率',
        |    cdc_delete_flag string COMMENT 'CDC删除标志',
        |    cdc_dt string COMMENT 'CDC分区日期'
        |) USING HUDI
        |PARTITIONED BY (cdc_dt)
        |TBLPROPERTIES (
        |    'hoodie.datasource.write.table.type' = 'COPY_ON_WRITE',
        |    'hoodie.datasource.write.keygenerator.class' = 'org.apache.hudi.keygen.SimpleKeyGenerator',
        |    'hoodie.table.recordkey.fields' = 'id',
        |    'hoodie.datasource.write.operation' = 'upsert',
        |    'hoodie.datasource.insert.dup.policy' = 'insert',
        |    'hoodie.datasource.write.precombine.field' = 'update_time',
        |    'hoodie.datasource.write.partitionpath.field' = 'cdc_dt',
        |    'hoodie.clustering.inline' = 'true',
        |    'hoodie.index.type' = 'BUCKET',
        |    'hoodie.index.bucket.engine' = 'SIMPLE',
        |    'hoodie.bucket.index.num.buckets' = '3',
        |    'hoodie.cleaner.commits.retained' = '24',
        |    'hoodie.insert.shuffle.parallelism' = '10',
        |    'hoodie.upsert.shuffle.parallelism' = '10',
        |    'hoodie.bulkinsert.shuffle.parallelism' = '500'
        |)
        |""".stripMargin
      
      // 执行创建表的DDL，必须调用collect()才能触发执行
      spark.sql(createTableSql).collect()
      println("✓ Hudi表创建完成")
      
      // 3. 验证表创建成功
      println("3. 验证表创建成功...")
      val showTablesSql = "SHOW TABLES"
      val tables = spark.sql(showTablesSql).collect()
      
      println(s"✓ 数据库rtdw中的表数量: ${tables.length}")
      tables.foreach { row =>
        println(s"  - 表名: ${row.getString(1)}")
      }
      
      // 检查特定表是否存在
      val tableExists = tables.exists(_.getString(1) == "s009_t_alc_loan_bucket")
      if (tableExists) {
        println("✓ 表 s009_t_alc_loan_bucket 创建成功！")
        
        // 显示表结构
        println("4. 显示表结构...")
        val describeTableSql = "DESCRIBE TABLE s009_t_alc_loan_bucket"
        val tableSchema = spark.sql(describeTableSql).collect()
        
        println(s"✓ 表字段数量: ${tableSchema.length}")
        println("表结构信息（前10个字段）:")
        tableSchema.take(10).foreach { row =>
          println(f"  ${row.getString(0)}%-30s ${row.getString(1)}%-20s ${row.getString(2)}")
        }
        
      } else {
        throw new RuntimeException("表创建失败！未找到表 s009_t_alc_loan_bucket")
      }
      
      println("✅ 所有测试步骤执行成功！")
      
    } catch {
      case e: Exception =>
        println(s"❌ 测试执行失败: ${e.getMessage}")
        e.printStackTrace()
        throw e
    }
  }
}
