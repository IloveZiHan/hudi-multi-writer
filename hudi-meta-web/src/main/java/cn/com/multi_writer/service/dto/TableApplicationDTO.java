package cn.com.multi_writer.service.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.swagger.v3.oas.annotations.media.Schema;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;
import java.time.LocalDateTime;
import java.util.Objects;

/**
 * Hudi表应用关联数据传输对象
 * 对应表: hoodie_meta_table_application
 */
@Schema(description = "Hudi表应用关联信息")
public class TableApplicationDTO {

    @Schema(description = "关联ID", example = "1")
    private Integer id;

    @NotBlank(message = "表ID不能为空")
    @Size(max = 100, message = "表ID长度不能超过100字符")
    @Schema(description = "表ID", example = "user_table", required = true)
    private String tableId;

    @NotBlank(message = "应用名称不能为空")
    @Size(max = 100, message = "应用名称长度不能超过100字符")
    @Schema(description = "应用名称", example = "user-service", required = true)
    private String applicationName;

    @NotBlank(message = "表类型不能为空")
    @Size(max = 100, message = "表类型长度不能超过100字符")
    @Schema(description = "表类型", example = "hudi", required = true)
    private String tableType;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    @Schema(description = "创建时间")
    private LocalDateTime createTime;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    @Schema(description = "更新时间")
    private LocalDateTime updateTime;

    public TableApplicationDTO() {
    }

    public TableApplicationDTO(Integer id, String tableId, String applicationName, 
                              String tableType, LocalDateTime createTime, LocalDateTime updateTime) {
        this.id = id;
        this.tableId = tableId;
        this.applicationName = applicationName;
        this.tableType = tableType;
        this.createTime = createTime;
        this.updateTime = updateTime;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableApplicationDTO that = (TableApplicationDTO) o;
        return Objects.equals(id, that.id) &&
               Objects.equals(tableId, that.tableId) &&
               Objects.equals(applicationName, that.applicationName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, tableId, applicationName);
    }

    @Override
    public String toString() {
        return "TableApplicationDTO{" +
                "id=" + id +
                ", tableId='" + tableId + '\'' +
                ", applicationName='" + applicationName + '\'' +
                ", tableType='" + tableType + '\'' +
                ", createTime=" + createTime +
                ", updateTime=" + updateTime +
                '}';
    }
} 