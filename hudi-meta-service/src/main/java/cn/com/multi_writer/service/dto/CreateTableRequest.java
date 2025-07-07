package cn.com.multi_writer.service.dto;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.util.Objects;

/**
 * 创建表请求对象
 */
public class CreateTableRequest {
    
    @NotBlank(message = "表ID不能为空")
    private String id;
    
    @NotBlank(message = "schema不能为空")
    private String schema;
    
    @NotNull(message = "分区标志不能为空")
    private Boolean isPartitioned;
    
    private String partitionExpr;
    
    private String tags;
    
    private String description;
    
    @NotBlank(message = "源数据库不能为空")
    private String sourceDb;
    
    @NotBlank(message = "源表名不能为空")
    private String sourceTable;
    
    private String targetTable;
    
    private String targetDb;
    
    @NotBlank(message = "数据库类型不能为空")
    private String dbType;
    
    private String hoodieConfig;
    
    // 默认构造函数
    public CreateTableRequest() {
    }
    
    // Getters and Setters
    public String getId() {
        return id;
    }
    
    public void setId(String id) {
        this.id = id;
    }
    
    public String getSchema() {
        return schema;
    }
    
    public void setSchema(String schema) {
        this.schema = schema;
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
        CreateTableRequest that = (CreateTableRequest) o;
        return Objects.equals(id, that.id);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
    
    @Override
    public String toString() {
        return "CreateTableRequest{" +
                "id='" + id + '\'' +
                ", schema='" + schema + '\'' +
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