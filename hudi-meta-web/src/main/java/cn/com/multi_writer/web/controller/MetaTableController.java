package cn.com.multi_writer.web.controller;

import cn.com.multi_writer.service.MetaTableService;
import cn.com.multi_writer.service.dto.*;
import cn.com.multi_writer.web.common.ApiResponse;
import cn.com.multi_writer.web.exception.BusinessException;
import cn.com.multi_writer.web.exception.ResourceNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.Positive;
import javax.validation.constraints.PositiveOrZero;
import java.util.List;
import java.util.Optional;

/**
 * Meta Table REST API 控制器
 * 提供所有表管理相关的HTTP接口
 */
@RestController
@RequestMapping("/api/meta/tables")
@Validated
public class MetaTableController {
    
    private static final Logger logger = LoggerFactory.getLogger(MetaTableController.class);
    
    @Autowired
    private MetaTableService metaTableService;
    
    /**
     * 获取所有表（分页）
     * @param page 页码（从0开始）
     * @param size 每页大小
     * @return 分页结果
     */
    @GetMapping
    public ApiResponse<PageResult<MetaTableDTO>> getAllTables(
            @RequestParam(defaultValue = "0") @PositiveOrZero int page,
            @RequestParam(defaultValue = "10") @Positive int size) {
        
        logger.info("获取所有表，页码: {}, 每页大小: {}", page, size);
        
        try {
            PageResult<MetaTableDTO> result = metaTableService.getAllTables(page, size);
            return ApiResponse.success(result);
        } catch (Exception e) {
            logger.error("获取所有表失败", e);
            throw new BusinessException("获取表列表失败: " + e.getMessage(), e);
        }
    }
    
    /**
     * 根据ID获取表详情
     * @param id 表ID
     * @return 表详情
     */
    @GetMapping("/{id}")
    public ApiResponse<MetaTableDTO> getTableById(@PathVariable @NotBlank String id) {
        logger.info("根据ID获取表详情: {}", id);
        
        try {
            Optional<MetaTableDTO> table = metaTableService.getTableById(id);
            if (!table.isPresent()) {
                throw new ResourceNotFoundException("表不存在: " + id);
            }
            return ApiResponse.success(table.get());
        } catch (ResourceNotFoundException e) {
            throw e;
        } catch (Exception e) {
            logger.error("根据ID获取表详情失败: {}", id, e);
            throw new BusinessException("获取表详情失败: " + e.getMessage(), e);
        }
    }
    
    /**
     * 创建新表
     * @param request 创建表请求
     * @return 创建结果
     */
    @PostMapping
    public ApiResponse<String> createTable(@Valid @RequestBody CreateTableRequest request) {
        logger.info("创建新表: {}", request.getId());
        
        try {
            boolean success = metaTableService.createTable(request);
            if (success) {
                return ApiResponse.created("表创建成功: " + request.getId());
            } else {
                throw new BusinessException("表创建失败");
            }
        } catch (IllegalArgumentException e) {
            throw new BusinessException(400, e.getMessage(), e);
        } catch (Exception e) {
            logger.error("创建表失败: {}", request.getId(), e);
            throw new BusinessException("创建表失败: " + e.getMessage(), e);
        }
    }
    
    /**
     * 更新表信息
     * @param id 表ID
     * @param request 更新请求
     * @return 更新结果
     */
    @PutMapping("/{id}")
    public ApiResponse<String> updateTable(@PathVariable @NotBlank String id, 
                                         @Valid @RequestBody UpdateTableRequest request) {
        logger.info("更新表信息: {}", id);
        
        try {
            boolean success = metaTableService.updateTable(id, request);
            if (success) {
                return ApiResponse.success("表更新成功: " + id);
            } else {
                throw new BusinessException("表更新失败");
            }
        } catch (IllegalArgumentException e) {
            throw new ResourceNotFoundException(e.getMessage());
        } catch (Exception e) {
            logger.error("更新表失败: {}", id, e);
            throw new BusinessException("更新表失败: " + e.getMessage(), e);
        }
    }
    
    /**
     * 删除表
     * @param id 表ID
     * @return 删除结果
     */
    @DeleteMapping("/{id}")
    public ApiResponse<String> deleteTable(@PathVariable @NotBlank String id) {
        logger.info("删除表: {}", id);
        
        try {
            boolean success = metaTableService.deleteTable(id);
            if (success) {
                return ApiResponse.success("表删除成功: " + id);
            } else {
                throw new BusinessException("表删除失败");
            }
        } catch (Exception e) {
            logger.error("删除表失败: {}", id, e);
            throw new BusinessException("删除表失败: " + e.getMessage(), e);
        }
    }
    
    /**
     * 高级搜索表
     * @param criteria 搜索条件
     * @param page 页码
     * @param size 每页大小
     * @return 搜索结果
     */
    @PostMapping("/search")
    public ApiResponse<PageResult<MetaTableDTO>> searchTables(
            @Valid @RequestBody SearchCriteria criteria,
            @RequestParam(defaultValue = "0") @PositiveOrZero int page,
            @RequestParam(defaultValue = "10") @Positive int size) {
        
        logger.info("高级搜索表，页码: {}, 每页大小: {}", page, size);
        
        try {
            PageResult<MetaTableDTO> result = metaTableService.searchTables(criteria, page, size);
            return ApiResponse.success(result);
        } catch (Exception e) {
            logger.error("搜索表失败", e);
            throw new BusinessException("搜索表失败: " + e.getMessage(), e);
        }
    }
    
    /**
     * 批量操作表
     * @param request 批量操作请求
     * @return 操作结果
     */
    @PostMapping("/batch")
    public ApiResponse<BatchOperationResult> batchOperation(@Valid @RequestBody BatchOperationRequest request) {
        logger.info("批量操作表，操作类型: {}", request.getOperation());
        
        try {
            BatchOperationResult result = metaTableService.batchOperation(request);
            return ApiResponse.success(result);
        } catch (Exception e) {
            logger.error("批量操作失败", e);
            throw new BusinessException("批量操作失败: " + e.getMessage(), e);
        }
    }
    
    /**
     * 获取表状态统计
     * @return 状态统计信息
     */
    @GetMapping("/stats")
    public ApiResponse<TableStatusStats> getTableStatusStats() {
        logger.info("获取表状态统计");
        
        try {
            TableStatusStats stats = metaTableService.getTableStatusStats();
            return ApiResponse.success(stats);
        } catch (Exception e) {
            logger.error("获取表状态统计失败", e);
            throw new BusinessException("获取表状态统计失败: " + e.getMessage(), e);
        }
    }
    
    /**
     * 表上线
     * @param id 表ID
     * @return 操作结果
     */
    @PostMapping("/{id}/online")
    public ApiResponse<String> onlineTable(@PathVariable @NotBlank String id) {
        logger.info("表上线: {}", id);
        
        try {
            boolean success = metaTableService.onlineTable(id);
            if (success) {
                return ApiResponse.success("表上线成功: " + id);
            } else {
                throw new BusinessException("表上线失败");
            }
        } catch (Exception e) {
            logger.error("表上线失败: {}", id, e);
            throw new BusinessException("表上线失败: " + e.getMessage(), e);
        }
    }
    
    /**
     * 表下线
     * @param id 表ID
     * @return 操作结果
     */
    @PostMapping("/{id}/offline")
    public ApiResponse<String> offlineTable(@PathVariable @NotBlank String id) {
        logger.info("表下线: {}", id);
        
        try {
            boolean success = metaTableService.offlineTable(id);
            if (success) {
                return ApiResponse.success("表下线成功: " + id);
            } else {
                throw new BusinessException("表下线失败");
            }
        } catch (Exception e) {
            logger.error("表下线失败: {}", id, e);
            throw new BusinessException("表下线失败: " + e.getMessage(), e);
        }
    }
    
    /**
     * 验证表schema
     * @param schema JSON schema字符串
     * @return 验证结果
     */
    @PostMapping("/validate-schema")
    public ApiResponse<String> validateTableSchema(@RequestBody @NotBlank String schema) {
        logger.info("验证表schema");
        
        try {
            boolean isValid = metaTableService.validateTableSchema(schema);
            if (isValid) {
                return ApiResponse.success("Schema验证通过");
            } else {
                throw new BusinessException("Schema验证失败");
            }
        } catch (Exception e) {
            logger.error("Schema验证失败", e);
            throw new BusinessException("Schema验证失败: " + e.getMessage(), e);
        }
    }
    
    /**
     * 获取支持的数据库类型
     * @return 数据库类型列表
     */
    @GetMapping("/db-types")
    public ApiResponse<List<String>> getSupportedDbTypes() {
        logger.info("获取支持的数据库类型");
        
        try {
            List<String> dbTypes = metaTableService.getSupportedDbTypes();
            return ApiResponse.success(dbTypes);
        } catch (Exception e) {
            logger.error("获取支持的数据库类型失败", e);
            throw new BusinessException("获取支持的数据库类型失败: " + e.getMessage(), e);
        }
    }
    
    /**
     * 检查表ID是否存在
     * @param id 表ID
     * @return 存在性检查结果
     */
    @GetMapping("/{id}/exists")
    public ApiResponse<Boolean> existsTableId(@PathVariable @NotBlank String id) {
        logger.info("检查表ID是否存在: {}", id);
        
        try {
            boolean exists = metaTableService.existsTableId(id);
            return ApiResponse.success(exists);
        } catch (Exception e) {
            logger.error("检查表ID存在性失败: {}", id, e);
            throw new BusinessException("检查表ID存在性失败: " + e.getMessage(), e);
        }
    }
    
    /**
     * 批量删除表
     * @param ids 表ID列表
     * @return 批量操作结果
     */
    @DeleteMapping("/batch")
    public ApiResponse<BatchOperationResult> batchDeleteTables(@RequestBody @NotEmpty List<String> ids) {
        logger.info("批量删除表，数量: {}", ids.size());
        
        try {
            BatchOperationResult result = metaTableService.batchDeleteTables(ids);
            return ApiResponse.success(result);
        } catch (Exception e) {
            logger.error("批量删除表失败", e);
            throw new BusinessException("批量删除表失败: " + e.getMessage(), e);
        }
    }
    
    /**
     * 批量上线表
     * @param ids 表ID列表
     * @return 批量操作结果
     */
    @PostMapping("/batch/online")
    public ApiResponse<BatchOperationResult> batchOnlineTables(@RequestBody @NotEmpty List<String> ids) {
        logger.info("批量上线表，数量: {}", ids.size());
        
        try {
            BatchOperationResult result = metaTableService.batchOnlineTables(ids);
            return ApiResponse.success(result);
        } catch (Exception e) {
            logger.error("批量上线表失败", e);
            throw new BusinessException("批量上线表失败: " + e.getMessage(), e);
        }
    }
    
    /**
     * 批量下线表
     * @param ids 表ID列表
     * @return 批量操作结果
     */
    @PostMapping("/batch/offline")
    public ApiResponse<BatchOperationResult> batchOfflineTables(@RequestBody @NotEmpty List<String> ids) {
        logger.info("批量下线表，数量: {}", ids.size());
        
        try {
            BatchOperationResult result = metaTableService.batchOfflineTables(ids);
            return ApiResponse.success(result);
        } catch (Exception e) {
            logger.error("批量下线表失败", e);
            throw new BusinessException("批量下线表失败: " + e.getMessage(), e);
        }
    }
    
    /**
     * 导出表数据
     * @param ids 表ID列表
     * @return 导出结果
     */
    @PostMapping("/export")
    public ApiResponse<BatchOperationResult> exportTables(@RequestBody @NotEmpty List<String> ids) {
        logger.info("导出表数据，数量: {}", ids.size());
        
        try {
            BatchOperationResult result = metaTableService.exportTables(ids);
            return ApiResponse.success(result);
        } catch (Exception e) {
            logger.error("导出表数据失败", e);
            throw new BusinessException("导出表数据失败: " + e.getMessage(), e);
        }
    }
    
    /**
     * 导出搜索结果
     * @param criteria 搜索条件
     * @return 导出结果
     */
    @PostMapping("/export/search")
    public ApiResponse<BatchOperationResult> exportSearchResults(@Valid @RequestBody SearchCriteria criteria) {
        logger.info("导出搜索结果");
        
        try {
            BatchOperationResult result = metaTableService.exportSearchResults(criteria);
            return ApiResponse.success(result);
        } catch (Exception e) {
            logger.error("导出搜索结果失败", e);
            throw new BusinessException("导出搜索结果失败: " + e.getMessage(), e);
        }
    }
    
    /**
     * 获取表Schema信息
     * @param id 表ID
     * @return Schema详情
     */
    @GetMapping("/{id}/schema")
    public ApiResponse<String> getTableSchema(@PathVariable @NotBlank String id) {
        logger.info("获取表Schema信息: {}", id);
        
        try {
            Optional<String> schema = metaTableService.getTableSchema(id);
            if (!schema.isPresent()) {
                throw new ResourceNotFoundException("表不存在: " + id);
            }
            return ApiResponse.success(schema.get());
        } catch (ResourceNotFoundException e) {
            throw e;
        } catch (Exception e) {
            logger.error("获取表Schema信息失败: {}", id, e);
            throw new BusinessException("获取表Schema信息失败: " + e.getMessage(), e);
        }
    }
    
    /**
     * 获取表历史记录
     * @param id 表ID
     * @return 历史记录列表
     */
    @GetMapping("/{id}/history")
    public ApiResponse<List<Object>> getTableHistory(@PathVariable @NotBlank String id) {
        logger.info("获取表历史记录: {}", id);
        
        try {
            List<Object> history = metaTableService.getTableHistory(id);
            return ApiResponse.success(history);
        } catch (Exception e) {
            logger.error("获取表历史记录失败: {}", id, e);
            throw new BusinessException("获取表历史记录失败: " + e.getMessage(), e);
        }
    }
    
    /**
     * 复制表配置
     * @param id 原表ID
     * @param newId 新表ID
     * @return 复制结果
     */
    @PostMapping("/{id}/copy")
    public ApiResponse<String> copyTable(@PathVariable @NotBlank String id, 
                                       @RequestParam @NotBlank String newId) {
        logger.info("复制表配置: {} -> {}", id, newId);
        
        try {
            boolean success = metaTableService.copyTable(id, newId);
            if (success) {
                return ApiResponse.success("表复制成功: " + id + " -> " + newId);
            } else {
                throw new BusinessException("表复制失败");
            }
        } catch (Exception e) {
            logger.error("复制表配置失败: {} -> {}", id, newId, e);
            throw new BusinessException("复制表配置失败: " + e.getMessage(), e);
        }
    }
    
    /**
     * 获取表使用统计
     * @param id 表ID
     * @return 使用统计
     */
    @GetMapping("/{id}/usage-stats")
    public ApiResponse<Object> getTableUsageStats(@PathVariable @NotBlank String id) {
        logger.info("获取表使用统计: {}", id);
        
        try {
            Object stats = metaTableService.getTableUsageStats(id);
            return ApiResponse.success(stats);
        } catch (Exception e) {
            logger.error("获取表使用统计失败: {}", id, e);
            throw new BusinessException("获取表使用统计失败: " + e.getMessage(), e);
        }
    }
} 