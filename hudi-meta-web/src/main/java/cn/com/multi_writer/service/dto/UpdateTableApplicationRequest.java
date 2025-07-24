package cn.com.multi_writer.service.dto;

import io.swagger.v3.oas.annotations.media.Schema;

import javax.validation.constraints.Size;

/**
 * 更新表应用关联请求对象
 */
@Schema(description = "更新表应用关联请求")
public class UpdateTableApplicationRequest {

    @Size(max = 100, message = "表ID长度不能超过100字符")
    @Schema(description = "表ID", example = "user_table")
    private String tableId;

    @Size(max = 100, message = "应用名称长度不能超过100字符")
    @Schema(description = "应用名称", example = "user-service")
    private String applicationName;

    @Size(max = 100, message = "表类型长度不能超过100字符")
    @Schema(description = "表类型", example = "hudi")
    private String tableType;

    public UpdateTableApplicationRequest() {
    }

    public UpdateTableApplicationRequest(String tableId, String applicationName, String tableType) {
        this.tableId = tableId;
        this.applicationName = applicationName;
        this.tableType = tableType;
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

    @Override
    public String toString() {
        return "UpdateTableApplicationRequest{" +
                "tableId='" + tableId + '\'' +
                ", applicationName='" + applicationName + '\'' +
                ", tableType='" + tableType + '\'' +
                '}';
    }
} 