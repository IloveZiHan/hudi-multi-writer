package cn.com.multi_writer.web;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 * Hudi Meta Web应用程序启动类
 */
@SpringBootApplication
@ComponentScan(basePackages = "cn.com.multi_writer")
public class HudiMetaWebApplication {
    
    private static final Logger logger = LoggerFactory.getLogger(HudiMetaWebApplication.class);
    
    public static void main(String[] args) {
        logger.info("正在启动Hudi Meta Web应用程序...");
        SpringApplication.run(HudiMetaWebApplication.class, args);
        logger.info("Hudi Meta Web应用程序启动完成");
    }
}
