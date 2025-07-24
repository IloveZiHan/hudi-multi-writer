package cn.com.multi_writer.service.dto;

import io.swagger.v3.oas.annotations.media.Schema;

import javax.validation.constraints.Size;

/**
 * 应用程序搜索条件
 */
@Schema(description = "应用程序搜索条件")
public class ApplicationSearchCriteria {

    @Size(max = 255, message = "关键词长度不能超过255字符")
    @Schema(description = "关键词(搜索名称和描述)", example = "user")
    private String keyword;

    @Size(max = 100, message = "应用名称长度不能超过100字符")
    @Schema(description = "应用名称", example = "user-service")
    private String name;

    @Schema(description = "创建时间开始", example = "2023-01-01 00:00:00")
    private String createTimeStart;

    @Schema(description = "创建时间结束", example = "2023-12-31 23:59:59")
    private String createTimeEnd;

    public ApplicationSearchCriteria() {
    }

    public String getKeyword() {
        return keyword;
    }

    public void setKeyword(String keyword) {
        this.keyword = keyword;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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
        return "ApplicationSearchCriteria{" +
                "keyword='" + keyword + '\'' +
                ", name='" + name + '\'' +
                ", createTimeStart='" + createTimeStart + '\'' +
                ", createTimeEnd='" + createTimeEnd + '\'' +
                '}';
    }
} 