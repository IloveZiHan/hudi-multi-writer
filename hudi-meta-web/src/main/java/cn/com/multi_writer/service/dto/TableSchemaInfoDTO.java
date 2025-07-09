package cn.com.multi_writer.service.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;
import java.util.List;

/**
 * 表结构信息 DTO
 * 对应前端的 TableSchemaInfo 接口
 */
public class TableSchemaInfoDTO {

    /**
     * 表名
     */
    @NotBlank(message = "表名不能为空")
    @Size(max = 100, message = "表名长度不能超过100字符")
    private String tableName;

    /**
     * 数据库名
     */
    @NotBlank(message = "数据库名不能为空")
    @Size(max = 100, message = "数据库名长度不能超过100字符")
    private String database;

    /**
     * 表注释
     */
    @Size(max = 500, message = "表注释长度不能超过500字符")
    private String comment;

    /**
     * 表字段信息列表
     */
    private List<TableFieldInfoDTO> fields;

    /**
     * 主键字段名列表
     */
    private List<String> primaryKeys;

    public TableSchemaInfoDTO() {
    }

    public TableSchemaInfoDTO(String tableName, String database, String comment, 
                             List<TableFieldInfoDTO> fields, List<String> primaryKeys) {
        this.tableName = tableName;
        this.database = database;
        this.comment = comment;
        this.fields = fields;
        this.primaryKeys = primaryKeys;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public List<TableFieldInfoDTO> getFields() {
        return fields;
    }

    public void setFields(List<TableFieldInfoDTO> fields) {
        this.fields = fields;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public void setPrimaryKeys(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableSchemaInfoDTO that = (TableSchemaInfoDTO) o;
        return tableName != null ? tableName.equals(that.tableName) : that.tableName == null;
    }

    @Override
    public int hashCode() {
        return tableName != null ? tableName.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "TableSchemaInfoDTO{" +
                "tableName='" + tableName + '\'' +
                ", database='" + database + '\'' +
                ", comment='" + comment + '\'' +
                ", fields=" + fields +
                ", primaryKeys=" + primaryKeys +
                '}';
    }
} 