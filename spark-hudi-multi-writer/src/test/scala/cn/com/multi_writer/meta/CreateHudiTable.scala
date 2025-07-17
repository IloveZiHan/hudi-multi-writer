package cn.com.multi_writer.meta

import cn.com.multi_writer.SparkSessionManager

object CreateHudiTable {

    def main(args: Array[String]): Unit = {
        val spark = SparkSessionManager.createSparkSession("CreateHudiTable")

        spark.sql(
            """
CREATE TABLE IF NOT EXISTS default.sms_mcs_sms_record_week (
    sms_id string,
    app_channel string,
    sms_channel string,
    sms_sign string,
    sms_channel_type string,
    mobile string,
    model_code string,
    sms_content string,
    server_ip string,
    batch_no string,
    file_path string,
    file_name string,
    sms_sts string,
    create_time string,
    plan_time string,
    send_time string,
    send_complete_time string,
    send_spent_time string,
    total_spent_time string,
    sms_real_num string,
    return_id string,
    return_code string,
    final_code string,
    final_desc string,
    final_update_time string,
    final_done_time string,
    error_desc string,
    error_hd_sts string,
    error_hd_rmk string,
    callback_sts string,
    failed_resend_sts string,
    business_key string,
    uplink_code string,
    biz_sms_id string,
    id_no string,
    sms_price string,
    model_version string,
    operator string,
    area_code string,
    norm_code string,
    sharding_key string,
    resend_no string,
    stt_resend_id string,
    resend_type string,
    cdc_delete_flag string COMMENT 'CDC删除标志',
    cdc_dt string COMMENT 'CDC分区日期'
) USING HUDI
PARTITIONED BY (cdc_dt)
TBLPROPERTIES (
    'hoodie.table.name' = 'sms_mcs_sms_record_week',
    'hoodie.table.recordkey.fields' = 'sms_id',
    'hoodie.bucket.index.hash.field' = 'sms_id',
    'hoodie.insert.shuffle.parallelism' = '10',
    'hoodie.upsert.shuffle.parallelism' = '10',
    'hoodie.bucket.index.num.buckets' = '20',
    'hoodie.bulkinsert.shuffle.parallelism' = '500',
    'hoodie.index.bucket.engine' = 'SIMPLE',
    'hoodie.datasource.write.table.type' = 'COPY_ON_WRITE',
    'hoodie.table.keygenerator.class' = 'org.apache.hudi.keygen.SimpleKeyGenerator',
    'hoodie.datasource.write.recordkey.field' = 'sms_id',
    'hoodie.datasource.write.operation' = 'upsert',
    'hoodie.datasource.insert.dup.policy' = 'insert',
    'hoodie.datasource.meta.sync.enable' = 'false',
    'hoodie.datasource.meta_sync.condition.sync' = 'true',
    'hoodie.datasource.hive_sync.enable' = 'false',
    'hoodie.datasource.hive_sync.mode' = 'hms',
    'hoodie.datasource.hive_sync.metastore.uris' = 'hadoop1:9083',
    'hoodie.datasource.write.precombine.field' = 'send_time',
    'hoodie.datasource.write.partitionpath.field' = 'cdc_dt',
    'hoodie.datasource.write.hive_style_partitioning' = 'true',
    'hoodie.index.type' = 'BUCKET',
    'hoodie.cleaner.commits.retained' = '24'
);
    """).show()

    }
}
