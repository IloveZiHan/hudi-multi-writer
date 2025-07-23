-- Hudi元数据表DDL脚本
-- 创建数据库（如果不存在）
CREATE DATABASE IF NOT EXISTS rtdw_meta DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci;

-- 使用数据库
USE rtdw_meta;

-- 创建Hudi元数据表
CREATE TABLE IF NOT EXISTS `hoodie_meta_table` (
    `id` varchar(255) NOT NULL COMMENT '表ID，唯一标识',
    `schema` longtext NOT NULL COMMENT '表结构JSON',
    `status` tinyint(1) NOT NULL DEFAULT 0 COMMENT '表状态：0-离线，1-在线',
    `is_partitioned` tinyint(1) NOT NULL DEFAULT 0 COMMENT '是否分区表：0-否，1-是',
    `partition_expr` varchar(500) DEFAULT NULL COMMENT '分区表达式',
    `hoodie_config` longtext COMMENT 'Hudi配置JSON',
    `tags` varchar(1000) DEFAULT NULL COMMENT '标签，逗号分隔',
    `description` varchar(1000) DEFAULT NULL COMMENT '表描述',
    `source_db` varchar(255) DEFAULT NULL COMMENT '源数据库名',
    `source_table` varchar(255) DEFAULT NULL COMMENT '源表名',
    `db_type` varchar(50) DEFAULT NULL COMMENT '数据库类型',
    `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    `cdc_delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标志：0-正常，1-已删除',
    PRIMARY KEY (`id`),
    KEY `idx_status` (`status`),
    KEY `idx_source_db` (`source_db`),
    KEY `idx_db_type` (`db_type`),
    KEY `idx_create_time` (`create_time`),
    KEY `idx_update_time` (`update_time`),
    KEY `idx_cdc_delete_flag` (`cdc_delete_flag`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci COMMENT='Hudi元数据表';

-- 创建表历史记录表（可选）
CREATE TABLE IF NOT EXISTS `hoodie_meta_table_history` (
    `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '历史记录ID',
    `table_id` varchar(255) NOT NULL COMMENT '表ID',
    `operation` varchar(50) NOT NULL COMMENT '操作类型：CREATE/UPDATE/DELETE',
    `old_value` longtext COMMENT '原始值JSON',
    `new_value` longtext COMMENT '新值JSON',
    `operator` varchar(100) DEFAULT NULL COMMENT '操作者',
    `operation_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '操作时间',
    PRIMARY KEY (`id`),
    KEY `idx_table_id` (`table_id`),
    KEY `idx_operation` (`operation`),
    KEY `idx_operation_time` (`operation_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci COMMENT='Hudi元数据表历史记录';

-- 创建表使用统计表（可选）
CREATE TABLE IF NOT EXISTS `hoodie_meta_table_stats` (
    `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '统计ID',
    `table_id` varchar(255) NOT NULL COMMENT '表ID',
    `stat_date` date NOT NULL COMMENT '统计日期',
    `read_count` bigint(20) DEFAULT 0 COMMENT '读取次数',
    `write_count` bigint(20) DEFAULT 0 COMMENT '写入次数',
    `data_size` bigint(20) DEFAULT 0 COMMENT '数据大小（字节）',
    `record_count` bigint(20) DEFAULT 0 COMMENT '记录数',
    `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_table_date` (`table_id`, `stat_date`),
    KEY `idx_stat_date` (`stat_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci COMMENT='Hudi元数据表使用统计';

CREATE TABLE IF NOT EXISTS `hoodie_meta_db_server` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '代理主键',
  `name` varchar(100) NOT NULL COMMENT 'SOURCE数据库名',
  `host` varchar(100) NOT NULL COMMENT 'SOURCE数据库服务器IP',
  `source_type` varchar(100) NOT NULL COMMENT 'SOURCE数据库类型:mysql,oracle,tdsql...',
  `jdbc_url` varchar(100) NOT NULL COMMENT 'SOURCE数据库JDBC URL',
  `username` varchar(100) NOT NULL COMMENT 'SOURCE数据库服务器用户',
  `password` varchar(100) NOT NULL COMMENT 'SOURCE数据库服务器密码',
  `alias` varchar(100) DEFAULT NULL COMMENT '别名，例如：s009、s004',
  `description` text COMMENT '描述信息',
  `create_user` varchar(100) NOT NULL COMMENT '创建人',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_user` varchar(100) DEFAULT NULL COMMENT '更新人',
  `update_time` timestamp NULL DEFAULT NULL COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=25 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci COMMENT='数据库服务器表';

-- 创建Hudi表、应用关联表
CREATE TABLE IF NOT EXISTS `hoodie_meta_table_application` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '代理主键',
  `table_id` varchar(100) NOT NULL COMMENT '表ID',
  `application_name` varchar(100) NOT NULL COMMENT '应用名称',
  `table_type` varchar(100) NOT NULL COMMENT '表类型:hudi,kafka,es,jdbc...',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=25 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci COMMENT='Hudi表、应用关联表';


-- 创建应用程序表
CREATE TABLE IF NOT EXISTS `hoodie_meta_application` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '代理主键',
  `name` varchar(100) NOT NULL COMMENT '应用名称',
  `description` text COMMENT '描述信息',
  `conf` text COMMENT '配置信息',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` timestamp NULL DEFAULT NULL COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=25 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci COMMENT='应用程序表';