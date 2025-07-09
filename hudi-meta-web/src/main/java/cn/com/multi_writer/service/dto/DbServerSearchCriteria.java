package cn.com.multi_writer.service.dto;

import javax.validation.constraints.Size;

/**
 * 数据库服务器搜索条件 DTO
 */
public class DbServerSearchCriteria {

    @Size(max = 255, message = "关键词长度不能超过255字符")
    private String keyword;

    @Size(max = 20, message = "数据库类型长度不能超过20字符")
    private String sourceType;

    @Size(max = 100, message = "主机地址长度不能超过100字符")
    private String host;

    @Size(max = 100, message = "创建人长度不能超过100字符")
    private String createUser;

    private String createTimeStart;

    private String createTimeEnd;

    // 构造函数
    public DbServerSearchCriteria() {
    }

    // Getters and Setters
    public String getKeyword() {
        return keyword;
    }

    public void setKeyword(String keyword) {
        this.keyword = keyword;
    }

    public String getSourceType() {
        return sourceType;
    }

    public void setSourceType(String sourceType) {
        this.sourceType = sourceType;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getCreateUser() {
        return createUser;
    }

    public void setCreateUser(String createUser) {
        this.createUser = createUser;
    }

    public String getCreateTimeStart() {
        return createTimeStart;
    }

    public void setCreateTimeStart(String createTimeStart) {
        this.createTimeStart = createTimeStart;
    }

    public String getCreateTimeEnd() {
        return createTimeEnd;
    }

    public void setCreateTimeEnd(String createTimeEnd) {
        this.createTimeEnd = createTimeEnd;
    }

    @Override
    public String toString() {
        return "DbServerSearchCriteria{" +
                "keyword='" + keyword + '\'' +
                ", sourceType='" + sourceType + '\'' +
                ", host='" + host + '\'' +
                ", createUser='" + createUser + '\'' +
                ", createTimeStart='" + createTimeStart + '\'' +
                ", createTimeEnd='" + createTimeEnd + '\'' +
                '}';
    }
} 