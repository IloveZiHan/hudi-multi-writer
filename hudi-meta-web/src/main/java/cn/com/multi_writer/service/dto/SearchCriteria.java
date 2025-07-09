package cn.com.multi_writer.service.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import java.time.LocalDateTime;
import java.util.Objects;

/**
 * 搜索条件对象
 */
public class SearchCriteria {
    
    private String keyword; // 模糊搜索关键词
    
    private Integer status; // 状态筛选
    
    private String sourceDb; // 源数据库筛选
    
    private String targetDb; // 目标数据库筛选
    
    private String dbType; // 数据库类型筛选
    
    private Boolean isPartitioned; // 是否分区筛选
    
    private String tags; // 标签筛选
    
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime createTimeStart; // 创建时间范围开始
    
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime createTimeEnd; // 创建时间范围结束
    
    // 默认构造函数
    public SearchCriteria() {
    }
    
    // Getters and Setters
    public String getKeyword() {
        return keyword;
    }
    
    public void setKeyword(String keyword) {
        this.keyword = keyword;
    }
    
    public Integer getStatus() {
        return status;
    }
    
    public void setStatus(Integer status) {
        this.status = status;
    }
    
    public String getSourceDb() {
        return sourceDb;
    }
    
    public void setSourceDb(String sourceDb) {
        this.sourceDb = sourceDb;
    }
    
    public String getTargetDb() {
        return targetDb;
    }
    
    public void setTargetDb(String targetDb) {
        this.targetDb = targetDb;
    }
    
    public String getDbType() {
        return dbType;
    }
    
    public void setDbType(String dbType) {
        this.dbType = dbType;
    }
    
    public Boolean getIsPartitioned() {
        return isPartitioned;
    }
    
    public void setIsPartitioned(Boolean isPartitioned) {
        this.isPartitioned = isPartitioned;
    }
    
    public String getTags() {
        return tags;
    }
    
    public void setTags(String tags) {
        this.tags = tags;
    }
    
    public LocalDateTime getCreateTimeStart() {
        return createTimeStart;
    }
    
    public void setCreateTimeStart(LocalDateTime createTimeStart) {
        this.createTimeStart = createTimeStart;
    }
    
    public LocalDateTime getCreateTimeEnd() {
        return createTimeEnd;
    }
    
    public void setCreateTimeEnd(LocalDateTime createTimeEnd) {
        this.createTimeEnd = createTimeEnd;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchCriteria that = (SearchCriteria) o;
        return Objects.equals(keyword, that.keyword) &&
                Objects.equals(status, that.status) &&
                Objects.equals(sourceDb, that.sourceDb) &&
                Objects.equals(targetDb, that.targetDb) &&
                Objects.equals(dbType, that.dbType) &&
                Objects.equals(isPartitioned, that.isPartitioned) &&
                Objects.equals(tags, that.tags) &&
                Objects.equals(createTimeStart, that.createTimeStart) &&
                Objects.equals(createTimeEnd, that.createTimeEnd);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(keyword, status, sourceDb, targetDb, dbType, 
                isPartitioned, tags, createTimeStart, createTimeEnd);
    }
    
    @Override
    public String toString() {
        return "SearchCriteria{" +
                "keyword='" + keyword + '\'' +
                ", status=" + status +
                ", sourceDb='" + sourceDb + '\'' +
                ", targetDb='" + targetDb + '\'' +
                ", dbType='" + dbType + '\'' +
                ", isPartitioned=" + isPartitioned +
                ", tags='" + tags + '\'' +
                ", createTimeStart=" + createTimeStart +
                ", createTimeEnd=" + createTimeEnd +
                '}';
    }
} 