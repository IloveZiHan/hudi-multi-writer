package cn.com.multi_writer.service.dto;

import io.swagger.v3.oas.annotations.media.Schema;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;

/**
 * 创建应用程序请求对象
 */
@Schema(description = "创建应用程序请求")
public class CreateApplicationRequest {

    @NotBlank(message = "应用名称不能为空")
    @Size(max = 100, message = "应用名称长度不能超过100字符")
    @Schema(description = "应用名称", example = "user-service", required = true)
    private String name;

    @Size(max = 1000, message = "描述信息长度不能超过1000字符")
    @Schema(description = "描述信息", example = "用户服务应用程序")
    private String description;

    @Schema(description = "配置信息", example = "{\"key\": \"value\"}")
    private String conf;

    public CreateApplicationRequest() {
    }

    public CreateApplicationRequest(String name, String description, String conf) {
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
        return "CreateApplicationRequest{" +
                "name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", conf='" + conf + '\'' +
                '}';
    }
} 