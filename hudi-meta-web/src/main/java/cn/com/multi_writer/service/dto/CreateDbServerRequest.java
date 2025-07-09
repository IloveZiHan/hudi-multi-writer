package cn.com.multi_writer.service.dto;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;

/**
 * 创建数据库服务器请求 DTO
 */
public class CreateDbServerRequest {

    @NotBlank(message = "数据库名称不能为空")
    @Size(max = 100, message = "数据库名称长度不能超过100字符")
    private String name;

    @NotBlank(message = "服务器地址不能为空")
    @Size(max = 100, message = "服务器地址长度不能超过100字符")
    private String host;

    @NotBlank(message = "数据库类型不能为空")
    @Size(max = 20, message = "数据库类型长度不能超过20字符")
    private String sourceType;

    @NotBlank(message = "JDBC URL不能为空")
    @Size(max = 500, message = "JDBC URL长度不能超过500字符")
    private String jdbcUrl;

    @NotBlank(message = "用户名不能为空")
    @Size(max = 100, message = "用户名长度不能超过100字符")
    private String username;

    @NotBlank(message = "密码不能为空")
    @Size(max = 100, message = "密码长度不能超过100字符")
    private String password;

    @Size(max = 100, message = "别名长度不能超过100字符")
    private String alias;

    @Size(max = 1000, message = "描述长度不能超过1000字符")
    private String description;

    @NotBlank(message = "创建人不能为空")
    @Size(max = 100, message = "创建人长度不能超过100字符")
    private String createUser;

    // 构造函数
    public CreateDbServerRequest() {
    }

    // Getters and Setters
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getSourceType() {
        return sourceType;
    }

    public void setSourceType(String sourceType) {
        this.sourceType = sourceType;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getCreateUser() {
        return createUser;
    }

    public void setCreateUser(String createUser) {
        this.createUser = createUser;
    }

    @Override
    public String toString() {
        return "CreateDbServerRequest{" +
                "name='" + name + '\'' +
                ", host='" + host + '\'' +
                ", sourceType='" + sourceType + '\'' +
                ", jdbcUrl='" + jdbcUrl + '\'' +
                ", username='" + username + '\'' +
                ", alias='" + alias + '\'' +
                ", description='" + description + '\'' +
                ", createUser='" + createUser + '\'' +
                '}';
    }
} 