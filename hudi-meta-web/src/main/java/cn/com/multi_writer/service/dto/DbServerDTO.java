package cn.com.multi_writer.service.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;
import java.time.LocalDateTime;

/**
 * 数据库服务器 DTO
 * 对应 hoodie_meta_db_server 表
 */
public class DbServerDTO {

    private Integer id;

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

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime createTime;

    @Size(max = 100, message = "更新人长度不能超过100字符")
    private String updateUser;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime updateTime;

    public DbServerDTO() {
    }

    public DbServerDTO(Integer id, String name, String host, String sourceType, String jdbcUrl, 
                       String username, String password, String alias, String description, 
                       String createUser, LocalDateTime createTime, String updateUser, 
                       LocalDateTime updateTime) {
        this.id = id;
        this.name = name;
        this.host = host;
        this.sourceType = sourceType;
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.alias = alias;
        this.description = description;
        this.createUser = createUser;
        this.createTime = createTime;
        this.updateUser = updateUser;
        this.updateTime = updateTime;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

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

    public LocalDateTime getCreateTime() {
        return createTime;
    }

    public void setCreateTime(LocalDateTime createTime) {
        this.createTime = createTime;
    }

    public String getUpdateUser() {
        return updateUser;
    }

    public void setUpdateUser(String updateUser) {
        this.updateUser = updateUser;
    }

    public LocalDateTime getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(LocalDateTime updateTime) {
        this.updateTime = updateTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DbServerDTO that = (DbServerDTO) o;
        return id != null ? id.equals(that.id) : that.id == null;
    }

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "DbServerDTO{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", host='" + host + '\'' +
                ", sourceType='" + sourceType + '\'' +
                ", jdbcUrl='" + jdbcUrl + '\'' +
                ", username='" + username + '\'' +
                ", alias='" + alias + '\'' +
                ", description='" + description + '\'' +
                ", createUser='" + createUser + '\'' +
                ", createTime=" + createTime +
                ", updateUser='" + updateUser + '\'' +
                ", updateTime=" + updateTime +
                '}';
    }
} 