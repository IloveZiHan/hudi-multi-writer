package cn.com.multi_writer.service.dto;

import io.swagger.v3.oas.annotations.media.Schema;

import javax.validation.constraints.Size;

/**
 * 表应用关联搜索条件
 */
@Schema(description = "表应用关联搜索条件")
public class TableApplicationSearchCriteria {

    @Size(max = 255, message = "关键词长度不能超过255字符")
    @Schema(description = "关键词(搜索表ID或应用名称)", example = "user")
    private String keyword;

    @Size(max = 100, message = "表ID长度不能超过100字符")
    @Schema(description = "表ID", example = "user_table")
    private String tableId;

    @Size(max = 100, message = "应用名称长度不能超过100字符")
    @Schema(description = "应用名称", example = "user-service")
    private String applicationName;

    @Size(max = 100, message = "表类型长度不能超过100字符")
    @Schema(description = "表类型", example = "hudi")
    private String tableType;

    @Schema(description = "创建时间开始", example = "2023-01-01 00:00:00")
    private String createTimeStart;

    @Schema(description = "创建时间结束", example = "2023-12-31 23:59:59")
    private String createTimeEnd;

    public TableApplicationSearchCriteria() {
    }

    public String getKeyword() {
        return keyword;
    }

    public void setKeyword(String keyword) {
        this.keyword = keyword;
    }

    public String getTableId() {
        return tableId;
    }

    public void setTableId(String tableId) {
        this.tableId = tableId;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public void setApplicationName(String applicationName) {
        this.applicationName = applicationName;
    }

    public String getTableType() {
        return tableType;
    }

    public void setTableType(String tableType) {
        this.tableType = tableType;
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
        return "TableApplicationSearchCriteria{" +
                "keyword='" + keyword + '\'' +
                ", tableId='" + tableId + '\'' +
                ", applicationName='" + applicationName + '\'' +
                ", tableType='" + tableType + '\'' +
                ", createTimeStart='" + createTimeStart + '\'' +
                ", createTimeEnd='" + createTimeEnd + '\'' +
                '}';
    }
} 