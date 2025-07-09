package cn.com.multi_writer.service.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;
import java.time.LocalDateTime;

/**
 * 业务表信息 DTO
 * 对应业务表的基本信息
 */
public class BusinessTableInfoDTO {

    /**
     * 表名
     */
    @NotBlank(message = "表名不能为空")
    @Size(max = 100, message = "表名长度不能超过100字符")
    private String tableName;

    /**
     * 表注释
     */
    @Size(max = 500, message = "表注释长度不能超过500字符")
    private String tableComment;

    /**
     * 表结构schema
     */
    private String schema;

    /**
     * 数据库名
     */
    @NotBlank(message = "数据库名不能为空")
    @Size(max = 100, message = "数据库名长度不能超过100字符")
    private String database;

    /**
     * 存储引擎
     */
    @Size(max = 50, message = "存储引擎长度不能超过50字符")
    private String engine;

    /**
     * 创建时间
     */
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime createTime;

    /**
     * 更新时间
     */
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime updateTime;

    /**
     * 表类型
     */
    @Size(max = 50, message = "表类型长度不能超过50字符")
    private String tableType;

    /**
     * 表大小
     */
    private Long tableSize;

    /**
     * 行数
     */
    private Long rowCount;

    public BusinessTableInfoDTO() {
    }

    public BusinessTableInfoDTO(String tableName, String tableComment, String schema, String database,
                               String engine, LocalDateTime createTime, LocalDateTime updateTime,
                               String tableType, Long tableSize, Long rowCount) {
        this.tableName = tableName;
        this.tableComment = tableComment;
        this.schema = schema;
        this.database = database;
        this.engine = engine;
        this.createTime = createTime;
        this.updateTime = updateTime;
        this.tableType = tableType;
        this.tableSize = tableSize;
        this.rowCount = rowCount;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableComment() {
        return tableComment;
    }

    public void setTableComment(String tableComment) {
        this.tableComment = tableComment;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getEngine() {
        return engine;
    }

    public void setEngine(String engine) {
        this.engine = engine;
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

    public String getTableType() {
        return tableType;
    }

    public void setTableType(String tableType) {
        this.tableType = tableType;
    }

    public Long getTableSize() {
        return tableSize;
    }

    public void setTableSize(Long tableSize) {
        this.tableSize = tableSize;
    }

    public Long getRowCount() {
        return rowCount;
    }

    public void setRowCount(Long rowCount) {
        this.rowCount = rowCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BusinessTableInfoDTO that = (BusinessTableInfoDTO) o;
        return tableName != null ? tableName.equals(that.tableName) : that.tableName == null;
    }

    @Override
    public int hashCode() {
        return tableName != null ? tableName.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "BusinessTableInfoDTO{" +
                "tableName='" + tableName + '\'' +
                ", tableComment='" + tableComment + '\'' +
                ", database='" + database + '\'' +
                ", engine='" + engine + '\'' +
                ", createTime=" + createTime +
                ", updateTime=" + updateTime +
                ", tableType='" + tableType + '\'' +
                ", tableSize=" + tableSize +
                ", rowCount=" + rowCount +
                '}';
    }
} 