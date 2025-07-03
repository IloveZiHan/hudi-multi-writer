package cn.hnzycfc.hudi

import cn.com.multi_writer.SparkSessionManager
import org.apache.spark.sql.SparkSession
import org.junit.{After, Before, Test}
import org.slf4j.{Logger, LoggerFactory}

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
    private val logger: Logger = LoggerFactory.getLogger(classOf[CreateHudiTableTest])

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

        logger.info("=== Hudi表创建测试开始 ===")
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
        logger.info("=== Hudi表创建测试结束 ===")
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
            logger.info("开始执行Hudi表创建DDL...")

            // 1. 删除已存在的表（如果存在）
            logger.info("1. 删除已存在的表（如果存在）...")
            val dropTableSql = "DROP TABLE IF EXISTS s009_t_alc_loan"
            spark.sql(dropTableSql).collect()
            logger.info("✓ 表删除完成")

            // 2. 创建Hudi表
            logger.info("2. 创建Hudi表...")
            val createTableSql =
                """
                  |CREATE TABLE IF NOT EXISTS s009_t_alc_loan (
                  |    id STRING NOT NULL COMMENT '物理主键',
                  |    loan_no STRING NOT NULL COMMENT '借据号',
                  |    cont_no STRING COMMENT '合同号',
                  |    cust_no STRING COMMENT '客户编号',
                  |    cust_name STRING COMMENT '客户姓名',
                  |    id_type STRING COMMENT '证件类型',
                  |    id_no STRING COMMENT '证件号码',
                  |    prod_no STRING COMMENT '产品编号',
                  |    prod_id STRING COMMENT '产品ID',
                  |    trade_dt STRING COMMENT '交易日期',
                  |    loan_sts STRING COMMENT '借据状态',
                  |    int_start_dt STRING COMMENT '起息日',
                  |    loan_term INT COMMENT '贷款期限',
                  |    due_dt STRING COMMENT '到期日',
                  |    repay_dt INT COMMENT '还款日',
                  |    repay_intrv_unit STRING COMMENT '还款间隔单位',
                  |    repay_intrv INT COMMENT '还款间隔',
                  |    repay_mode_code STRING COMMENT '还款方式代码',
                  |    instm_ind STRING COMMENT '期供标志',
                  |    loan_tnr INT COMMENT '贷款期数',
                  |    loan_account_sts STRING COMMENT '借据账务状态',
                  |    fst_repay_dt STRING COMMENT '第一次扣款日',
                  |    last_repay_dt STRING COMMENT '上次还款日',
                  |    curr_repay_dt STRING COMMENT '当前还款日',
                  |    next_repay_dt STRING COMMENT '下一还款日',
                  |    five_lvl_class_code STRING COMMENT '五级分类编码',
                  |    int_rate DECIMAL(16,10) COMMENT '执行利率',
                  |    od_int_rate DECIMAL(16,10) COMMENT '罚息利率',
                  |    loan_amt DECIMAL(16,2) COMMENT '贷款金额',
                  |    loan_bal DECIMAL(16,2) COMMENT '贷款余额',
                  |    wrtoff_dt STRING COMMENT '核销日期',
                  |    grace_perd_type STRING COMMENT '宽限期类型',
                  |    grace_perd_days INT COMMENT '宽限期天数',
                  |    calc_od_int_part STRING COMMENT '计算罚息部分',
                  |    loan_deflt_ind STRING COMMENT '贷款拖欠标志',
                  |    last_provi_dt STRING COMMENT '上次计提日',
                  |    last_tran_over_dte STRING COMMENT '上次转逾期日',
                  |    last_deduct_dt STRING COMMENT '上一次扣款日',
                  |    curr_jon_penal_fee_dt STRING COMMENT '当前结违约金日期',
                  |    curr_jon_od_int_dt STRING COMMENT '当前结罚息日期',
                  |    last_provi_od_int_dt STRING COMMENT '上次计提罚息日期',
                  |    loan_deval_ind STRING COMMENT '贷款减值标志',
                  |    loan_deval_dt STRING COMMENT '贷款减值日期',
                  |    tran_off_bs_ind STRING COMMENT '转表外标志',
                  |    tran_off_bs_dt STRING COMMENT '转表外日期',
                  |    ori_due_dt STRING COMMENT '原到期日',
                  |    vat_tax_rate DECIMAL(16,9) COMMENT '增值税税率',
                  |    loan_lock_sts STRING COMMENT '借据锁定状态',
                  |    deval_repay_seq STRING COMMENT '减值还款顺序',
                  |    coopr_cont_no STRING COMMENT '项目协议编号',
                  |    agent_repay_ind STRING COMMENT '代还款标志',
                  |    delay_pay_ind STRING COMMENT '延迟支付标志',
                  |    coopr_biz_seq STRING COMMENT '合作方业务流水',
                  |    chan_no STRING COMMENT '进件渠道',
                  |    prft_type STRING COMMENT '收益类型',
                  |    day_rate_calc_base STRING NOT NULL COMMENT '日利率天数计算基础：360/365',
                  |    last_sub STRING COMMENT '尾期是否补差: Y-是、N-否',
                  |    account_org STRING COMMENT '账务机构',
                  |    create_time TIMESTAMP NOT NULL COMMENT '创建时间',
                  |    create_user STRING COMMENT '创建人',
                  |    update_time TIMESTAMP NOT NULL COMMENT '修改时间',
                  |    update_user STRING COMMENT '修改人',
                  |    del_flag STRING NOT NULL COMMENT '逻辑删标志',
                  |    risk_five_lvl_class_code STRING COMMENT '风险认定五级分类编码',
                  |    unite_crdt_ind STRING COMMENT '联合贷标志 Y-是,N-否',
                  |    our_pty_contri_ratio DECIMAL(16,10) COMMENT '我方出资比例',
                  |    insure_ind STRING COMMENT '承保标志',
                  |    loan_bal_accum DECIMAL(16,2) COMMENT '借据余额积数',
                  |    portfolio_code STRING COMMENT '资产池编号',
                  |    confirmation_type STRING COMMENT '终止确认类型（Y出表，N不出表）',
                  |    fund_type STRING COMMENT '放款类型',
                  |    amort_loan_no STRING COMMENT '账单分期借据号',
                  |    comb_rate DECIMAL(16,10) COMMENT '综合年化利率'
                  |) USING HUDI
                  |LOCATION '/tmp/spark-warehouse/s009_t_alc_loan'
                  |TBLPROPERTIES (
                  |    'hoodie.bucket.index.num.buckets' = '20',
                  |    'hoodie.insert.shuffle.parallelism' = '10',
                  |    'hoodie.datasource.write.precombine.field' = 'update_time',
                  |    'hoodie.index.bucket.engine' = 'SIMPLE',
                  |    'hoodie.bucket.index.hash.field' = 'id',
                  |    'hoodie.index.type' = 'BUCKET',
                  |    'hoodie.datasource.write.operation' = 'upsert',
                  |    'hoodie.datasource.insert.dup.policy' = 'insert',
                  |    'hoodie.datasource.write.table.type' = 'COPY_ON_WRITE',
                  |    'hoodie.datasource.write.hive_style_partitioning' = 'true',
                  |    'hoodie.datasource.write.keygenerator.class' = 'org.apache.hudi.keygen.SimpleKeyGenerator',
                  |    'hoodie.upsert.shuffle.parallelism' = '10',
                  |    'hoodie.cleaner.commits.retained' = '24',
                  |    'hoodie.table.recordkey.fields' = 'id',
                  |    'hoodie.datasource.write.partitionpath.field' = 'cdc_dt',
                  |    'hoodie.bulkinsert.shuffle.parallelism' = '500'
                  |)
                """.stripMargin

            // 执行创建表的DDL，必须调用collect()才能触发执行
            spark.sql(createTableSql).collect()
            logger.info("✓ Hudi表创建完成")

            logger.info("✅ 所有测试步骤执行成功！")

        } catch {
            case e: Exception =>
                logger.error(s"❌ 测试执行失败: ${e.getMessage}", e)
                throw e
        }
    }

    @Test
    def testCreateHudiTable1(): Unit = {
        try {
            logger.info("开始执行Hudi表创建DDL...")
            val ddl =
                """
                  |CREATE TABLE IF NOT EXISTS s009_t_alc_batch_repay (
                  |    id STRING NOT NULL COMMENT '物理主键',
                  |    trade_dt STRING COMMENT '交易日',
                  |    account_seq BIGINT COMMENT '交易流水/账务流水',
                  |    batch_no STRING COMMENT '支付批量代扣批次号',
                  |    loan_no STRING COMMENT '借据号',
                  |    bank_cd STRING COMMENT '银行代码 ',
                  |    prod_no STRING COMMENT '产品编号',
                  |    bank_name STRING COMMENT '银行名称',
                  |    card_no STRING COMMENT '卡号',
                  |    acct_type STRING COMMENT '账户类型',
                  |    acct_name STRING COMMENT '户名',
                  |    id_type STRING COMMENT '证件类型',
                  |    id_no STRING COMMENT '证件号码',
                  |    prcp_amt DECIMAL(16,2) COMMENT '本金',
                  |    int_amt DECIMAL(16,2) COMMENT '利息',
                  |    od_int_amt DECIMAL(16,2) COMMENT '罚息',
                  |    comp_int_amt DECIMAL(16,2) COMMENT '复利',
                  |    fee_amt DECIMAL(16,2) COMMENT '费用（包含违约金、周期性手续费）',
                  |    paym_amt DECIMAL(16,2) COMMENT '扣款金额',
                  |    actl_repay_amt DECIMAL(16,2) COMMENT '实际扣款金额',
                  |    batch_sts STRING COMMENT '状态',
                  |    rev_seq BIGINT COMMENT '冲正流水号',
                  |    deduct_chan STRING COMMENT '扣款渠道',
                  |    sms_ind STRING COMMENT '短信发送标志',
                  |    split_ind STRING COMMENT '拆单标志',
                  |    split_num INT COMMENT '拆单份数',
                  |    err_code STRING COMMENT '支付平台错误码',
                  |    err_msg STRING COMMENT '错误原因',
                  |    out_err_code STRING COMMENT '外部错误码',
                  |    out_err_msg STRING COMMENT '外部错误描述',
                  |    duct_day DECIMAL(16,2) COMMENT '支付平台返回单日限额',
                  |    hs_pay_no STRING COMMENT '备付金模式清算款核算编号',
                  |    hs_act_no STRING COMMENT '支付结算账号',
                  |    night_ind STRING COMMENT '是否晚间批扣',
                  |    create_time TIMESTAMP NOT NULL COMMENT '创建时间',
                  |    create_user STRING COMMENT '创建人',
                  |    update_time TIMESTAMP NOT NULL COMMENT '修改时间',
                  |    update_user STRING COMMENT '修改人',
                  |    del_flag STRING NOT NULL COMMENT '逻辑删标志',
                  |    chan_no STRING COMMENT '借据进件渠道',
                  |    exer_insure_ptnr_chan STRING COMMENT '履约险审批渠道号',
                  |    send_level STRING COMMENT '发送优先级别',
                  |    insure_fee_amt DECIMAL(16,2) COMMENT '保证险归还保费',
                  |    pay_insure_money DECIMAL(16,2) COMMENT '保证险还款清分保费金额',
                  |    dk_type STRING COMMENT '扣款支付类型',
                  |    cont_no STRING COMMENT '合同号',
                  |    group_sms_ind STRING COMMENT '合并短信发送标志',
                  |    pay_srl_no STRING COMMENT '支付通道流水号',
                  |    over_days INT COMMENT '逾期天数',
                  |    freq_code STRING COMMENT '类型码值',
                  |    suppress_flag STRING COMMENT '保证险还款保费押账标识',
                  |    suppress_amt DECIMAL(16,2) COMMENT '保证险还款保费押账金额',
                  |    split_config_id STRING COMMENT '限额拆单配置Id',
                  |    try_deduct_ind STRING COMMENT '是否试扣',
                  |    try_deduct_amt DECIMAL(16,2) COMMENT '试扣金额',
                  |    small_config_id STRING COMMENT '小金额衰减配置id',
                  |    fps_small_amt_flag STRING COMMENT '支付通道足额且按费率收费标识'
                  |) USING HUDI
                  |LOCATION '/tmp/spark-warehouse/s009_t_alc_batch_repay'
                  |PARTITIONED BY (cdc_dt String)
                  |TBLPROPERTIES (
                  |    'hoodie.bucket.index.num.buckets' = '20',
                  |    'hoodie.insert.shuffle.parallelism' = '10',
                  |    'hoodie.datasource.write.precombine.field' = 'update_time',
                  |    'hoodie.index.bucket.engine' = 'SIMPLE',
                  |    'hoodie.bucket.index.hash.field' = 'id',
                  |    'hoodie.index.type' = 'BUCKET',
                  |    'hoodie.datasource.write.operation' = 'upsert',
                  |    'hoodie.datasource.hive_sync.enable' = 'true',
                  |    'hoodie.datasource.insert.dup.policy' = 'insert',
                  |    'hoodie.datasource.write.table.type' = 'COPY_ON_WRITE',
                  |    'hoodie.datasource.write.hive_style_partitioning' = 'true',
                  |    'hoodie.datasource.write.keygenerator.class' = 'org.apache.hudi.keygen.SimpleKeyGenerator',
                  |    'hoodie.upsert.shuffle.parallelism' = '10',
                  |    'hoodie.cleaner.commits.retained' = '24',
                  |    'hoodie.table.recordkey.fields' = 'id',
                  |    'hoodie.datasource.write.partitionpath.field' = 'cdc_dt',
                  |    'hoodie.bulkinsert.shuffle.parallelism' = '500'
                  |);
                  |""".stripMargin
            spark.sql(ddl).collect()
            logger.info("✓ Hudi表创建完成")

            logger.info("✅ 所有测试步骤执行成功！")

        }
    }
}
