package cn.com.multi_writer.web.controller;

import cn.com.multi_writer.service.MetaTableService;
import cn.com.multi_writer.web.common.ApiResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * 健康检查控制器
 * 提供系统健康状态检查接口
 */
@Tag(name = "系统健康检查", description = "提供系统健康状态检查相关接口")
@RestController
@RequestMapping("/api/health")
public class HealthController {
    
    private static final Logger logger = LoggerFactory.getLogger(HealthController.class);
    
    @Autowired
    @Qualifier("mySQLMetaTableService")
    private MetaTableService metaTableService;
    
    @Value("${spring.application.name:hudi-meta-web}")
    private String applicationName;
    
    /**
     * 基础健康检查
     * @return 健康状态
     */
    @Operation(summary = "基础健康检查", description = "获取系统基础健康状态信息")
    @ApiResponses({
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "200", description = "健康检查成功"),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "500", description = "服务器内部错误")
    })
    @GetMapping
    public ApiResponse<Map<String, Object>> health() {
        Map<String, Object> healthInfo = new HashMap<>();
        healthInfo.put("status", "UP");
        healthInfo.put("application", applicationName);
        healthInfo.put("timestamp", LocalDateTime.now());
        
        return ApiResponse.success(healthInfo);
    }
    
    /**
     * 详细健康检查
     * @return 详细健康状态
     */
    @Operation(summary = "详细健康检查", description = "获取系统详细健康状态信息，包括服务状态和数据库类型支持")
    @ApiResponses({
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "200", description = "健康检查成功"),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "500", description = "服务器内部错误")
    })
    @GetMapping("/detail")
    public ApiResponse<Map<String, Object>> healthDetail() {
        Map<String, Object> healthInfo = new HashMap<>();
        healthInfo.put("application", applicationName);
        healthInfo.put("timestamp", LocalDateTime.now());
        
        // 检查服务状态
        boolean serviceHealthy = checkServiceHealth();
        healthInfo.put("service", serviceHealthy ? "UP" : "DOWN");
        
        // 检查数据库类型支持
        try {
            int dbTypesCount = metaTableService.getSupportedDbTypes().size();
            healthInfo.put("supportedDbTypes", dbTypesCount);
        } catch (Exception e) {
            logger.warn("无法获取支持的数据库类型: {}", e.getMessage());
            healthInfo.put("supportedDbTypes", "UNKNOWN");
        }
        
        // 总体状态
        boolean overall = serviceHealthy;
        healthInfo.put("status", overall ? "UP" : "DOWN");
        
        return ApiResponse.success(healthInfo);
    }
    
    /**
     * 检查服务健康状态
     * @return 服务是否健康
     */
    private boolean checkServiceHealth() {
        try {
            // 尝试调用服务方法来检查服务状态
            metaTableService.getSupportedDbTypes();
            return true;
        } catch (Exception e) {
            logger.error("服务健康检查失败: {}", e.getMessage(), e);
            return false;
        }
    }
} 