package cn.com.multi_writer.service.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;

/**
 * 表字段信息 DTO
 * 对应前端的 TableFieldInfo 接口
 */
public class TableFieldInfoDTO {

    /**
     * 字段名
     */
    @NotBlank(message = "字段名不能为空")
    @Size(max = 100, message = "字段名长度不能超过100字符")
    private String fieldName;

    /**
     * 字段类型
     */
    @NotBlank(message = "字段类型不能为空")
    @Size(max = 100, message = "字段类型长度不能超过100字符")
    private String fieldType;

    /**
     * 是否可为空
     */
    private boolean isNullable;

    /**
     * 默认值
     */
    @Size(max = 500, message = "默认值长度不能超过500字符")
    private String defaultValue;

    /**
     * 字段注释
     */
    @Size(max = 500, message = "字段注释长度不能超过500字符")
    private String comment;

    /**
     * 是否为主键
     */
    private boolean isPrimaryKey;

    public TableFieldInfoDTO() {
    }

    public TableFieldInfoDTO(String fieldName, String fieldType, boolean isNullable, 
                            String defaultValue, String comment, boolean isPrimaryKey) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.isNullable = isNullable;
        this.defaultValue = defaultValue;
        this.comment = comment;
        this.isPrimaryKey = isPrimaryKey;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getFieldType() {
        return fieldType;
    }

    public void setFieldType(String fieldType) {
        this.fieldType = fieldType;
    }

    public boolean isNullable() {
        return isNullable;
    }

    public void setNullable(boolean nullable) {
        isNullable = nullable;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    public void setPrimaryKey(boolean primaryKey) {
        isPrimaryKey = primaryKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableFieldInfoDTO that = (TableFieldInfoDTO) o;
        return fieldName != null ? fieldName.equals(that.fieldName) : that.fieldName == null;
    }

    @Override
    public int hashCode() {
        return fieldName != null ? fieldName.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "TableFieldInfoDTO{" +
                "fieldName='" + fieldName + '\'' +
                ", fieldType='" + fieldType + '\'' +
                ", isNullable=" + isNullable +
                ", defaultValue='" + defaultValue + '\'' +
                ", comment='" + comment + '\'' +
                ", isPrimaryKey=" + isPrimaryKey +
                '}';
    }
} 