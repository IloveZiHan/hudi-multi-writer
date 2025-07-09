-- Hudi元数据表DDL语句
-- 用于管理Hudi表的元数据信息，包括表结构schema、状态、标签等信息
-- 支持多个DDL语句，使用分号分隔

-- 创建元数据表
CREATE TABLE IF NOT EXISTS _hoodie_meta_table (
    id STRING NOT NULL COMMENT '表唯一标识符',
    schema STRING NOT NULL COMMENT '表结构JSON字符串',
    status INT NOT NULL COMMENT '表状态：0-未上线，1-已上线',
    is_partitioned BOOLEAN NOT NULL COMMENT '是否为分区表：true-分区表，false-非分区表',
    partition_expr STRING COMMENT '分区表达式，表示如何来计算分区值的',
    hoodie_config STRING COMMENT 'Hudi表配置的JSON串，用于保存创建表时Hudi表的配置',
    tags STRING COMMENT '表标签，多个标签用逗号分隔',
    description STRING COMMENT '表描述信息',
    source_db STRING COMMENT '源数据库名称',
    source_table STRING COMMENT '源表名称',
    db_type STRING COMMENT '数据库类型：MySQL、PostgreSQL、Oracle等',
    create_time TIMESTAMP NOT NULL COMMENT '创建时间',
    update_time TIMESTAMP NOT NULL COMMENT '更新时间',
    cdc_delete_flag INT NOT NULL COMMENT 'CDC删除标志：0-正常，1-删除'
) USING HUDI
LOCATION '/tmp/spark-warehouse/_hoodie_meta/_hoodie_meta_table'
TBLPROPERTIES (
    'hoodie.table.name' = '_hoodie_meta_table',
    'hoodie.datasource.write.recordkey.field' = 'id',
    'hoodie.datasource.write.precombine.field' = 'update_time',
    'hoodie.datasource.write.table.name' = '_hoodie_meta_table',
    'hoodie.datasource.write.operation' = 'upsert',
    'hoodie.datasource.write.table.type' = 'COPY_ON_WRITE',
    'hoodie.datasource.write.hive_style_partitioning' = 'false',
    'hoodie.index.type' = 'BLOOM',
    'hoodie.bloom.index.bucketized.checking' = 'true',
    'hoodie.datasource.write.payload.class' = 'org.apache.hudi.common.model.PartialUpdateAvroPayload'
);

-- 创建元数据表的视图（可选）
CREATE OR REPLACE VIEW _hoodie_meta_view AS
SELECT 
    id,
    schema,
    CASE 
        WHEN status = 0 THEN '未上线'
        WHEN status = 1 THEN '已上线'
        ELSE '未知状态'
    END as status_desc,
    is_partitioned,
    partition_expr,
    tags,
    description,
    source_db,
    source_table,
    db_type,
    create_time,
    update_time
FROM _hoodie_meta_table
WHERE cdc_delete_flag = 0;