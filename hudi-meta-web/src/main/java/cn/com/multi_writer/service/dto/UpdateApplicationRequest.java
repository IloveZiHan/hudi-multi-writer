package cn.com.multi_writer.service.dto;

import io.swagger.v3.oas.annotations.media.Schema;

import javax.validation.constraints.Size;

/**
 * 更新应用程序请求对象
 */
@Schema(description = "更新应用程序请求")
public class UpdateApplicationRequest {

    @Size(max = 100, message = "应用名称长度不能超过100字符")
    @Schema(description = "应用名称", example = "user-service")
    private String name;

    @Size(max = 1000, message = "描述信息长度不能超过1000字符")
    @Schema(description = "描述信息", example = "用户服务应用程序")
    private String description;

    @Schema(description = "配置信息", example = "{\"key\": \"value\"}")
    private String conf;

    public UpdateApplicationRequest() {
    }

    public UpdateApplicationRequest(String name, String description, String conf) {
        this.name = name;
        this.description = description;
        this.conf = conf;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getConf() {
        return conf;
    }

    public void setConf(String conf) {
        this.conf = conf;
    }

    @Override
    public String toString() {
        return "UpdateApplicationRequest{" +
                "name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", conf='" + conf + '\'' +
                '}';
    }
} 