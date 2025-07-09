package cn.com.multi_writer.web.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * 数据库初始化器
 * 在应用启动时自动执行DDL脚本，初始化数据库表结构
 */
@Component
public class DatabaseInitializer implements CommandLineRunner {
    
    private static final Logger logger = LoggerFactory.getLogger(DatabaseInitializer.class);
    
    @Autowired
    private DataSource dataSource;
    
    @Override
    public void run(String... args) throws Exception {
        logger.info("开始初始化数据库表结构...");
        
        try {
            // 从DDL文件读取SQL语句
            List<String> sqlStatements = loadDdlStatements();
            
            // 依次执行每个SQL语句
            try (Connection connection = dataSource.getConnection();
                 Statement stmt = connection.createStatement()) {
                
                for (String sql : sqlStatements) {
                    if (sql.trim().isEmpty()) {
                        continue; // 跳过空语句
                    }
                    
                    try {
                        logger.debug("执行SQL: {}", sql.substring(0, Math.min(100, sql.length())));
                        stmt.execute(sql);
                    } catch (Exception e) {
                        // 对于某些可能重复的操作（如CREATE TABLE IF NOT EXISTS），允许失败
                        if (e.getMessage().contains("already exists") || 
                            e.getMessage().contains("Duplicate key") ||
                            e.getMessage().contains("exists")) {
                            logger.debug("SQL语句执行失败（可能已存在）: {}", e.getMessage());
                        } else {
                            logger.error("SQL语句执行失败: {}", sql, e);
                            throw e;
                        }
                    }
                }
            }
            
            logger.info("数据库表结构初始化完成");
            
        } catch (Exception e) {
            logger.error("初始化数据库表结构失败", e);
            throw new RuntimeException("初始化数据库表结构失败: " + e.getMessage(), e);
        }
    }
    
    /**
     * 从DDL文件加载SQL语句
     * @return SQL语句列表
     */
    private List<String> loadDdlStatements() throws IOException {
        List<String> sqlStatements = new ArrayList<>();
        
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream("_hoodie_meta_ddl_mysql.sql")) {
            if (inputStream == null) {
                throw new RuntimeException("找不到DDL文件: _hoodie_meta_ddl_mysql.sql");
            }
            
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
                StringBuilder currentStatement = new StringBuilder();
                String line;
                
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    
                    // 跳过空行和注释行
                    if (line.isEmpty() || line.startsWith("--")) {
                        continue;
                    }
                    
                    // 累积当前语句
                    currentStatement.append(line).append(" ");
                    
                    // 如果行以分号结尾，说明是一个完整的语句
                    if (line.endsWith(";")) {
                        String statement = currentStatement.toString().trim();
                        if (!statement.isEmpty()) {
                            // 移除末尾的分号
                            if (statement.endsWith(";")) {
                                statement = statement.substring(0, statement.length() - 1);
                            }
                            sqlStatements.add(statement);
                        }
                        currentStatement = new StringBuilder();
                    }
                }
                
                // 处理最后一个语句（如果没有以分号结尾）
                String lastStatement = currentStatement.toString().trim();
                if (!lastStatement.isEmpty()) {
                    if (lastStatement.endsWith(";")) {
                        lastStatement = lastStatement.substring(0, lastStatement.length() - 1);
                    }
                    sqlStatements.add(lastStatement);
                }
            }
        }
        
        logger.info("从DDL文件加载了 {} 条SQL语句", sqlStatements.size());
        return sqlStatements;
    }
} 