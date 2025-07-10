package cn.com.multi_writer.web.controller;     

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import cn.com.multi_writer.web.common.ApiResponse;

@RestController
@RequestMapping("/api/hoodie-config")
public class HoodieConfigController {

    @GetMapping("/get-default-config")
    public ApiResponse<String> getDefaultConfig() throws IOException {
        // 从classpath读取hoodie-default.json文件
        InputStream inputStream = HoodieConfigController.class.getClassLoader().getResourceAsStream("hoodie-default.json");
        if (inputStream == null) {
            throw new IOException("默认配置文件未找到: hoodie-default.json");
        }
        
        String config = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        return ApiResponse.success(config);
    }
}
