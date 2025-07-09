package cn.com.multi_writer.service.dto;

import javax.validation.constraints.Size;

/**
 * 更新数据库服务器请求 DTO
 */
public class UpdateDbServerRequest {

    @Size(max = 100, message = "数据库名称长度不能超过100字符")
    private String name;

    @Size(max = 100, message = "服务器地址长度不能超过100字符")
    private String host;

    @Size(max = 20, message = "数据库类型长度不能超过20字符")
    private String sourceType;

    @Size(max = 500, message = "JDBC URL长度不能超过500字符")
    private String jdbcUrl;

    @Size(max = 100, message = "用户名长度不能超过100字符")
    private String username;

    @Size(max = 100, message = "密码长度不能超过100字符")
    private String password;

    @Size(max = 100, message = "别名长度不能超过100字符")
    private String alias;

    @Size(max = 1000, message = "描述长度不能超过1000字符")
    private String description;

    @Size(max = 100, message = "更新人长度不能超过100字符")
    private String updateUser;

    // 构造函数
    public UpdateDbServerRequest() {
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

    public String getUpdateUser() {
        return updateUser;
    }

    public void setUpdateUser(String updateUser) {
        this.updateUser = updateUser;
    }

    @Override
    public String toString() {
        return "UpdateDbServerRequest{" +
                "name='" + name + '\'' +
                ", host='" + host + '\'' +
                ", sourceType='" + sourceType + '\'' +
                ", jdbcUrl='" + jdbcUrl + '\'' +
                ", username='" + username + '\'' +
                ", alias='" + alias + '\'' +
                ", description='" + description + '\'' +
                ", updateUser='" + updateUser + '\'' +
                '}';
    }
} 