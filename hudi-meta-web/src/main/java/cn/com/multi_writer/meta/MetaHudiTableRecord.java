package cn.com.multi_writer.meta;

import java.sql.Timestamp;
import java.util.Objects;

/**
 * Hudi表元数据记录的Java POJO类
 * 
 * 对应元数据表中的一条记录，包含表的所有元数据信息
 */
public class MetaHudiTableRecord {
    
    /**
     * 表的唯一标识符
     */
    private String id;
    
    /**
     * 表结构的JSON字符串表示
     */
    private String schema;
    
    /**
     * 表状态，0-未上线，1-已上线
     */
    private int status;
    
    /**
     * 是否为分区表，true-分区表，false-非分区表
     */
    private boolean isPartitioned;
    
    /**
     * 分区表达式，表示如何来计算分区值的
     */
    private String partitionExpr;
    
    /**
     * Hudi表配置的JSON串，用于保存创建表时Hudi表的配置
     */
    private String hoodieConfig;
    
    /**
     * 表标签，多个标签用逗号分隔
     */
    private String tags;
    
    /**
     * 表描述信息
     */
    private String description;
    
    /**
     * 源数据库名称
     */
    private String sourceDb;
    
    /**
     * 源表名称
     */
    private String sourceTable;
    
    /**
     * 数据库类型（如：MySQL、PostgreSQL、Oracle等）
     */
    private String dbType;
    
    /**
     * 创建时间
     */
    private Timestamp createTime;
    
    /**
     * 更新时间
     */
    private Timestamp updateTime;

    /**
     * 是否删除
     */
    private int cdcDeleteFlag;  
    
    // 无参构造器
    public MetaHudiTableRecord() {}
    
    // 全参构造器
    public MetaHudiTableRecord(String id, String schema, int status, boolean isPartitioned, 
                               String partitionExpr, String hoodieConfig, String tags, 
                               String description, String sourceDb, String sourceTable, 
                               String dbType, Timestamp createTime, Timestamp updateTime, int cdcDeleteFlag) {
        this.id = id;
        this.schema = schema;
        this.status = status;
        this.isPartitioned = isPartitioned;
        this.partitionExpr = partitionExpr;
        this.hoodieConfig = hoodieConfig;
        this.tags = tags;
        this.description = description;
        this.sourceDb = sourceDb;
        this.sourceTable = sourceTable;
        this.dbType = dbType;
        this.createTime = createTime;
        this.updateTime = updateTime;
        this.cdcDeleteFlag = cdcDeleteFlag;
    }
    
    // Getter和Setter方法
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
    
    public int getStatus() {
        return status;
    }
    
    public void setStatus(int status) {
        this.status = status;
    }
    
    public boolean isPartitioned() {
        return isPartitioned;
    }
    
    public void setPartitioned(boolean partitioned) {
        isPartitioned = partitioned;
    }
    
    public String getPartitionExpr() {
        return partitionExpr;
    }
    
    public void setPartitionExpr(String partitionExpr) {
        this.partitionExpr = partitionExpr;
    }
    
    public String getHoodieConfig() {
        return hoodieConfig;
    }
    
    public void setHoodieConfig(String hoodieConfig) {
        this.hoodieConfig = hoodieConfig;
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
    
    public String getDbType() {
        return dbType;
    }
    
    public void setDbType(String dbType) {
        this.dbType = dbType;
    }
    
    public Timestamp getCreateTime() {
        return createTime;
    }
    
    public void setCreateTime(Timestamp createTime) {
        this.createTime = createTime;
    }
    
    public Timestamp getUpdateTime() {
        return updateTime;
    }
    
    public void setUpdateTime(Timestamp updateTime) {
        this.updateTime = updateTime;
    }
    
    public int getCdcDeleteFlag() {
        return cdcDeleteFlag;
    }
    
    public void setCdcDeleteFlag(int cdcDeleteFlag) {
        this.cdcDeleteFlag = cdcDeleteFlag;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MetaHudiTableRecord that = (MetaHudiTableRecord) o;
        return status == that.status &&
               isPartitioned == that.isPartitioned &&
               Objects.equals(id, that.id) &&
               Objects.equals(schema, that.schema) &&
               Objects.equals(partitionExpr, that.partitionExpr) &&
               Objects.equals(hoodieConfig, that.hoodieConfig) &&
               Objects.equals(tags, that.tags) &&
               Objects.equals(description, that.description) &&
               Objects.equals(sourceDb, that.sourceDb) &&
               Objects.equals(sourceTable, that.sourceTable) &&
               Objects.equals(dbType, that.dbType) &&
               Objects.equals(createTime, that.createTime) &&
               Objects.equals(updateTime, that.updateTime) &&
               cdcDeleteFlag == that.cdcDeleteFlag;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(id, schema, status, isPartitioned, partitionExpr, hoodieConfig, 
                           tags, description, sourceDb, sourceTable, dbType, createTime, updateTime);
    }
    
    @Override
    public String toString() {
        return "MetaHudiTableRecord{" +
               "id='" + id + '\'' +
               ", schema='" + (schema != null ? schema.substring(0, Math.min(schema.length(), 100)) + "..." : "null") + '\'' +
               ", status=" + status +
               ", isPartitioned=" + isPartitioned +
               ", partitionExpr='" + partitionExpr + '\'' +
               ", hoodieConfig='" + (hoodieConfig != null ? hoodieConfig.substring(0, Math.min(hoodieConfig.length(), 100)) + "..." : "null") + '\'' +
               ", tags='" + tags + '\'' +
               ", description='" + description + '\'' +
               ", sourceDb='" + sourceDb + '\'' +
               ", sourceTable='" + sourceTable + '\'' +
               ", dbType='" + dbType + '\'' +
               ", createTime=" + createTime +
               ", updateTime=" + updateTime +
               '}';
    }
} 