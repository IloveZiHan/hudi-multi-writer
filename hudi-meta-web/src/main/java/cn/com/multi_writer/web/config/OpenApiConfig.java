package cn.com.multi_writer.web.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * OpenAPI 3.0 配置类
 * 使用SpringDoc替代Springfox，提供更好的Spring Boot兼容性
 */
@Configuration
public class OpenApiConfig {
    
    @Value("${springdoc.api-docs.title:Hudi Meta Web API}")
    private String title;
    
    @Value("${springdoc.api-docs.description:Hudi元数据管理系统Web API接口文档}")
    private String description;
    
    @Value("${springdoc.api-docs.version:1.0.0}")
    private String version;
    
    @Value("${springdoc.api-docs.contact.name:Hudi Multi Writer Team}")
    private String contactName;
    
    @Value("${springdoc.api-docs.contact.url:https://github.com/hudi-multi-writer}")
    private String contactUrl;
    
    @Value("${springdoc.api-docs.contact.email:support@hudi-multi-writer.com}")
    private String contactEmail;
    
    @Value("${springdoc.api-docs.license.name:Apache License 2.0}")
    private String licenseName;
    
    @Value("${springdoc.api-docs.license.url:http://www.apache.org/licenses/LICENSE-2.0}")
    private String licenseUrl;

    /**
     * 配置OpenAPI信息
     * @return OpenAPI实例
     */
    @Bean
    public OpenAPI customOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title(title)
                        .description(description)
                        .version(version)
                        .contact(new Contact()
                                .name(contactName)
                                .url(contactUrl)
                                .email(contactEmail))
                        .license(new License()
                                .name(licenseName)
                                .url(licenseUrl)));
    }
} 