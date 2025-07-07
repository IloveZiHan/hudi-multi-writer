package cn.com.multi_writer.service.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.time.LocalDateTime;
import java.util.Objects;

/**
 * Meta Hudi Table 数据传输对象
 */
public class MetaTableDTO {
    
    private String id;
    
    @NotBlank(message = "schema不能为空")
    private String schema;
    
    @NotNull(message = "状态不能为空")
    private Integer status; // 0-未上线, 1-已上线
    
    @NotNull(message = "分区标志不能为空")
    @JsonProperty("isPartitioned")
    private Boolean isPartitioned;
    
    private String partitionExpr;
    
    @Size(max = 500, message = "标签长度不能超过500字符")
    private String tags;
    
    @Size(max = 1000, message = "描述长度不能超过1000字符")
    private String description;
    
    private String sourceDb;
    
    private String sourceTable;
    
    private String targetTable;
    
    private String targetDb;
    
    private String dbType;
    
    private String hoodieConfig;
    
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime createTime;
    
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime updateTime;
    
    private String creator;
    
    private String updater;
    
    // 默认构造函数
    public MetaTableDTO() {
    }
    
    // 完整构造函数
    public MetaTableDTO(String id, String schema, Integer status, Boolean isPartitioned, 
                        String partitionExpr, String tags, String description, 
                        String sourceDb, String sourceTable, String targetTable, 
                        String targetDb, String dbType, String hoodieConfig, 
                        LocalDateTime createTime, LocalDateTime updateTime, 
                        String creator, String updater) {
        this.id = id;
        this.schema = schema;
        this.status = status;
        this.isPartitioned = isPartitioned;
        this.partitionExpr = partitionExpr;
        this.tags = tags;
        this.description = description;
        this.sourceDb = sourceDb;
        this.sourceTable = sourceTable;
        this.targetTable = targetTable;
        this.targetDb = targetDb;
        this.dbType = dbType;
        this.hoodieConfig = hoodieConfig;
        this.createTime = createTime;
        this.updateTime = updateTime;
        this.creator = creator;
        this.updater = updater;
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
    
    public LocalDateTime getCreateTime() {
        return createTime;
    }
    
    public void setCreateTime(LocalDateTime createTime) {
        this.createTime = createTime;
    }
    
    public LocalDateTime getUpdateTime() {
        return updateTime;
    }
    
    public void setUpdateTime(LocalDateTime updateTime) {
        this.updateTime = updateTime;
    }
    
    public String getCreator() {
        return creator;
    }
    
    public void setCreator(String creator) {
        this.creator = creator;
    }
    
    public String getUpdater() {
        return updater;
    }
    
    public void setUpdater(String updater) {
        this.updater = updater;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MetaTableDTO that = (MetaTableDTO) o;
        return Objects.equals(id, that.id);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
    
    @Override
    public String toString() {
        return "MetaTableDTO{" +
                "id='" + id + '\'' +
                ", schema='" + schema + '\'' +
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
                ", createTime=" + createTime +
                ", updateTime=" + updateTime +
                ", creator='" + creator + '\'' +
                ", updater='" + updater + '\'' +
                '}';
    }
} 