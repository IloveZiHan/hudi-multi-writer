package cn.com.multi_writer.web.config;

import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;

/**
 * 数据源配置类
 * 管理HikariCP数据源Bean，用于Hudi元数据表的数据库连接
 */
@Configuration
public class DataSourceConfig {
    
    private static final Logger logger = LoggerFactory.getLogger(DataSourceConfig.class);
    
    /**
     * 创建主数据源Bean
     * 使用HikariCP作为连接池实现
     * 
     * @return 配置好的数据源
     */
    @Bean
    @Primary
    @ConfigurationProperties(prefix = "spring.datasource")
    public DataSource dataSource() {
        logger.info("正在创建HikariCP数据源...");
        
        HikariDataSource dataSource = DataSourceBuilder.create()
                .type(HikariDataSource.class)
                .build();
        
        logger.info("HikariCP数据源创建成功，连接池类型: {}", dataSource.getClass().getSimpleName());
        return dataSource;
    }
} 