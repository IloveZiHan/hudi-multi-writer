package cn.com.multi_writer.service.dto;

import java.util.Objects;

/**
 * 更新表请求对象
 */
public class UpdateTableRequest {
    
    private String schema;
    
    private Integer status;
    
    private Boolean isPartitioned;
    
    private String partitionExpr;
    
    private String tags;
    
    private String description;
    
    private String sourceDb;
    
    private String sourceTable;
    
    private String targetTable;
    
    private String targetDb;
    
    private String dbType;
    
    private String hoodieConfig;
    
    // 默认构造函数
    public UpdateTableRequest() {
    }
    
    // Getters and Setters
    public String getSchema() {
        return schema;
    }
    
    public void setSchema(String schema) {
        this.schema = schema;
    }
    
    public Integer getStatus() {
        return status;
    }
    
    public void setStatus(Integer status) {
        this.status = status;
    }
    
    public Boolean getIsPartitioned() {
        return isPartitioned;
    }
    
    public void setIsPartitioned(Boolean isPartitioned) {
        this.isPartitioned = isPartitioned;
    }
    
    public String getPartitionExpr() {
        return partitionExpr;
    }
    
    public void setPartitionExpr(String partitionExpr) {
        this.partitionExpr = partitionExpr;
    }
    
    public String getTags() {
        return tags;
    }
    
    public void setTags(String tags) {
        this.tags = tags;
    }
    
    public String getDescription() {
        return description;
    }
    
    public void setDescription(String description) {
        this.description = description;
    }
    
    public String getSourceDb() {
        return sourceDb;
    }
    
    public void setSourceDb(String sourceDb) {
        this.sourceDb = sourceDb;
    }
    
    public String getSourceTable() {
        return sourceTable;
    }
    
    public void setSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
    }
    
    public String getTargetTable() {
        return targetTable;
    }
    
    public void setTargetTable(String targetTable) {
        this.targetTable = targetTable;
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
    
    public String getHoodieConfig() {
        return hoodieConfig;
    }
    
    public void setHoodieConfig(String hoodieConfig) {
        this.hoodieConfig = hoodieConfig;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UpdateTableRequest that = (UpdateTableRequest) o;
        return Objects.equals(schema, that.schema) &&
                Objects.equals(status, that.status) &&
                Objects.equals(isPartitioned, that.isPartitioned) &&
                Objects.equals(partitionExpr, that.partitionExpr) &&
                Objects.equals(tags, that.tags) &&
                Objects.equals(description, that.description) &&
                Objects.equals(sourceDb, that.sourceDb) &&
                Objects.equals(sourceTable, that.sourceTable) &&
                Objects.equals(targetTable, that.targetTable) &&
                Objects.equals(targetDb, that.targetDb) &&
                Objects.equals(dbType, that.dbType) &&
                Objects.equals(hoodieConfig, that.hoodieConfig);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(schema, status, isPartitioned, partitionExpr, tags,
                description, sourceDb, sourceTable, targetTable, targetDb, dbType, hoodieConfig);
    }
    
    @Override
    public String toString() {
        return "UpdateTableRequest{" +
                "schema='" + schema + '\'' +
                ", status=" + status +
                ", isPartitioned=" + isPartitioned +
                ", partitionExpr='" + partitionExpr + '\'' +
                ", tags='" + tags + '\'' +
                ", description='" + description + '\'' +
                ", sourceDb='" + sourceDb + '\'' +
                ", sourceTable='" + sourceTable + '\'' +
                ", targetTable='" + targetTable + '\'' +
                ", targetDb='" + targetDb + '\'' +
                ", dbType='" + dbType + '\'' +
                ", hoodieConfig='" + hoodieConfig + '\'' +
                '}';
    }
} 