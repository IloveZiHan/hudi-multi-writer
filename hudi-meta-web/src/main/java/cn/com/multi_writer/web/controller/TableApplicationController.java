package cn.com.multi_writer.web.controller;

import cn.com.multi_writer.service.TableApplicationService;
import cn.com.multi_writer.service.dto.*;
import cn.com.multi_writer.web.common.ApiResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Positive;
import javax.validation.constraints.PositiveOrZero;
import java.util.List;
import java.util.Optional;

/**
 * 表应用关联管理控制器
 * 提供表应用关联的CRUD操作和管理功能
 */
@Tag(name = "表应用关联管理", description = "提供表应用关联的CRUD操作和管理功能")
@RestController
@RequestMapping("/api/table-applications")
@Validated
public class TableApplicationController {

    private static final Logger logger = LoggerFactory.getLogger(TableApplicationController.class);

    @Autowired
    @Qualifier("mySQLTableApplicationService")
    private TableApplicationService tableApplicationService;

    /**
     * 获取所有表应用关联
     */
    @Operation(summary = "获取所有表应用关联", description = "分页获取所有表应用关联信息")
    @GetMapping
    public ApiResponse<PageResult<TableApplicationDTO>> getAllTableApplications(
            @Parameter(description = "页码(从0开始)", example = "0") @RequestParam(defaultValue = "0") @PositiveOrZero int page,
            @Parameter(description = "每页大小", example = "10") @RequestParam(defaultValue = "10") @Positive int size) {
        
        try {
            PageResult<TableApplicationDTO> result = tableApplicationService.getAllTableApplications(page, size);
            logger.info("获取表应用关联列表成功，页码: {}, 每页大小: {}, 总数: {}", page, size, result.getTotal());
            return ApiResponse.success(result);
        } catch (Exception e) {
            logger.error("获取表应用关联列表失败", e);
            return ApiResponse.internalError("获取表应用关联列表失败: " + e.getMessage());
        }
    }

    /**
     * 根据ID获取表应用关联详情
     */
    @Operation(summary = "根据ID获取表应用关联详情", description = "根据关联ID获取详细的表应用关联信息")
    @GetMapping("/{id}")
    public ApiResponse<TableApplicationDTO> getTableApplicationById(@Parameter(description = "关联ID", example = "1") @PathVariable @Positive Integer id) {
        try {
            Optional<TableApplicationDTO> tableApplication = tableApplicationService.getTableApplicationById(id);
            if (tableApplication.isPresent()) {
                logger.info("获取表应用关联详情成功: ID={}", id);
                return ApiResponse.success(tableApplication.get());
            } else {
                logger.warn("表应用关联不存在: ID={}", id);
                return ApiResponse.notFound("表应用关联不存在");
            }
        } catch (Exception e) {
            logger.error("获取表应用关联详情失败: ID={}", id, e);
            return ApiResponse.internalError("获取表应用关联详情失败: " + e.getMessage());
        }
    }

    /**
     * 创建新的表应用关联
     */
    @Operation(summary = "创建新的表应用关联", description = "创建新的表应用关联")
    @PostMapping
    public ApiResponse<String> createTableApplication(@Valid @RequestBody CreateTableApplicationRequest request) {
        try {
            // 检查关联是否已存在
            if (tableApplicationService.existsTableApplicationRelation(request.getTableId(), request.getApplicationName())) {
                logger.warn("表应用关联已存在: tableId={}, applicationName={}", 
                          request.getTableId(), request.getApplicationName());
                return ApiResponse.conflict("表应用关联已存在");
            }

            boolean success = tableApplicationService.createTableApplication(request);
            if (success) {
                logger.info("创建表应用关联成功: tableId={}, applicationName={}", 
                          request.getTableId(), request.getApplicationName());
                return ApiResponse.created("创建表应用关联成功", "表应用关联创建完成");
            } else {
                logger.error("创建表应用关联失败: tableId={}, applicationName={}", 
                           request.getTableId(), request.getApplicationName());
                return ApiResponse.internalError("创建表应用关联失败");
            }
        } catch (Exception e) {
            logger.error("创建表应用关联异常: tableId={}, applicationName={}", 
                        request.getTableId(), request.getApplicationName(), e);
            return ApiResponse.internalError("创建表应用关联异常: " + e.getMessage());
        }
    }

    /**
     * 更新表应用关联信息
     */
    @Operation(summary = "更新表应用关联信息", description = "更新指定表应用关联的信息")
    @PutMapping("/{id}")
    public ApiResponse<String> updateTableApplication(@Parameter(description = "关联ID", example = "1") @PathVariable @Positive Integer id,
                                                     @Valid @RequestBody UpdateTableApplicationRequest request) {
        try {
            // 检查关联是否存在
            if (!tableApplicationService.existsTableApplicationId(id)) {
                logger.warn("表应用关联不存在: ID={}", id);
                return ApiResponse.notFound("表应用关联不存在");
            }

            boolean success = tableApplicationService.updateTableApplication(id, request);
            if (success) {
                logger.info("更新表应用关联成功: ID={}", id);
                return ApiResponse.success("更新表应用关联成功");
            } else {
                logger.error("更新表应用关联失败: ID={}", id);
                return ApiResponse.internalError("更新表应用关联失败");
            }
        } catch (Exception e) {
            logger.error("更新表应用关联异常: ID={}", id, e);
            return ApiResponse.internalError("更新表应用关联异常: " + e.getMessage());
        }
    }

    /**
     * 删除表应用关联
     */
    @Operation(summary = "删除表应用关联", description = "删除指定的表应用关联")
    @DeleteMapping("/{id}")
    public ApiResponse<String> deleteTableApplication(@Parameter(description = "关联ID", example = "1") @PathVariable @Positive Integer id) {
        try {
            // 检查关联是否存在
            if (!tableApplicationService.existsTableApplicationId(id)) {
                logger.warn("表应用关联不存在: ID={}", id);
                return ApiResponse.notFound("表应用关联不存在");
            }

            boolean success = tableApplicationService.deleteTableApplication(id);
            if (success) {
                logger.info("删除表应用关联成功: ID={}", id);
                return ApiResponse.success("删除表应用关联成功");
            } else {
                logger.error("删除表应用关联失败: ID={}", id);
                return ApiResponse.internalError("删除表应用关联失败");
            }
        } catch (Exception e) {
            logger.error("删除表应用关联异常: ID={}", id, e);
            return ApiResponse.internalError("删除表应用关联异常: " + e.getMessage());
        }
    }

    /**
     * 搜索表应用关联
     */
    @Operation(summary = "搜索表应用关联", description = "根据条件搜索表应用关联")
    @PostMapping("/search")
    public ApiResponse<PageResult<TableApplicationDTO>> searchTableApplications(
            @Valid @RequestBody TableApplicationSearchCriteria criteria,
            @Parameter(description = "页码(从0开始)", example = "0") @RequestParam(defaultValue = "0") @PositiveOrZero int page,
            @Parameter(description = "每页大小", example = "10") @RequestParam(defaultValue = "10") @Positive int size) {
        
        try {
            PageResult<TableApplicationDTO> result = tableApplicationService.searchTableApplications(criteria, page, size);
            logger.info("搜索表应用关联成功，页码: {}, 每页大小: {}, 总数: {}", page, size, result.getTotal());
            return ApiResponse.success(result);
        } catch (Exception e) {
            logger.error("搜索表应用关联失败", e);
            return ApiResponse.internalError("搜索表应用关联失败: " + e.getMessage());
        }
    }

    /**
     * 批量删除表应用关联
     */
    @Operation(summary = "批量删除表应用关联", description = "批量删除指定的表应用关联")
    @DeleteMapping("/batch")
    public ApiResponse<BatchOperationResult> batchDeleteTableApplications(@RequestBody @NotEmpty List<Integer> ids) {
        try {
            BatchOperationResult result = tableApplicationService.batchDeleteTableApplications(ids);
            logger.info("批量删除表应用关联完成，成功: {}, 失败: {}, 总数: {}", 
                       result.getSuccess(), result.getFailed(), result.getTotal());
            return ApiResponse.success(result);
        } catch (Exception e) {
            logger.error("批量删除表应用关联异常", e);
            return ApiResponse.internalError("批量删除表应用关联异常: " + e.getMessage());
        }
    }

    /**
     * 检查表应用关联ID是否存在
     */
    @Operation(summary = "检查表应用关联ID是否存在", description = "检查指定ID的表应用关联是否存在")
    @GetMapping("/{id}/exists")
    public ApiResponse<Boolean> existsTableApplicationId(@Parameter(description = "关联ID", example = "1") @PathVariable @Positive Integer id) {
        try {
            boolean exists = tableApplicationService.existsTableApplicationId(id);
            logger.debug("检查表应用关联ID是否存在: ID={}, 结果={}", id, exists);
            return ApiResponse.success(exists);
        } catch (Exception e) {
            logger.error("检查表应用关联ID是否存在异常: ID={}", id, e);
            return ApiResponse.internalError("检查表应用关联ID是否存在异常: " + e.getMessage());
        }
    }

    /**
     * 根据表ID获取所有关联的应用程序
     */
    @Operation(summary = "根据表ID获取关联的应用程序", description = "根据表ID获取所有关联的应用程序")
    @GetMapping("/by-table/{tableId}")
    public ApiResponse<List<TableApplicationDTO>> getTableApplicationsByTableId(
            @Parameter(description = "表ID", example = "user_table") @PathVariable @NotBlank String tableId) {
        try {
            List<TableApplicationDTO> result = tableApplicationService.getTableApplicationsByTableId(tableId);
            logger.info("根据表ID获取关联的应用程序成功: tableId={}, 数量={}", tableId, result.size());
            return ApiResponse.success(result);
        } catch (Exception e) {
            logger.error("根据表ID获取关联的应用程序失败: tableId={}", tableId, e);
            return ApiResponse.internalError("根据表ID获取关联的应用程序失败: " + e.getMessage());
        }
    }

    /**
     * 根据应用名称获取所有关联的表
     */
    @Operation(summary = "根据应用名称获取关联的表", description = "根据应用名称获取所有关联的表")
    @GetMapping("/by-application/{applicationName}")
    public ApiResponse<List<TableApplicationDTO>> getTableApplicationsByApplicationName(
            @Parameter(description = "应用名称", example = "user-service") @PathVariable @NotBlank String applicationName) {
        try {
            List<TableApplicationDTO> result = tableApplicationService.getTableApplicationsByApplicationName(applicationName);
            logger.info("根据应用名称获取关联的表成功: applicationName={}, 数量={}", applicationName, result.size());
            return ApiResponse.success(result);
        } catch (Exception e) {
            logger.error("根据应用名称获取关联的表失败: applicationName={}", applicationName, e);
            return ApiResponse.internalError("根据应用名称获取关联的表失败: " + e.getMessage());
        }
    }

    /**
     * 根据表类型获取所有关联
     */
    @Operation(summary = "根据表类型获取关联", description = "根据表类型获取所有关联")
    @GetMapping("/by-table-type/{tableType}")
    public ApiResponse<List<TableApplicationDTO>> getTableApplicationsByTableType(
            @Parameter(description = "表类型", example = "hudi") @PathVariable @NotBlank String tableType) {
        try {
            List<TableApplicationDTO> result = tableApplicationService.getTableApplicationsByTableType(tableType);
            logger.info("根据表类型获取关联成功: tableType={}, 数量={}", tableType, result.size());
            return ApiResponse.success(result);
        } catch (Exception e) {
            logger.error("根据表类型获取关联失败: tableType={}", tableType, e);
            return ApiResponse.internalError("根据表类型获取关联失败: " + e.getMessage());
        }
    }

    /**
     * 检查特定表和应用的关联是否存在
     */
    @Operation(summary = "检查表应用关联是否存在", description = "检查特定表和应用的关联是否存在")
    @GetMapping("/relation/exists")
    public ApiResponse<Boolean> existsTableApplicationRelation(
            @Parameter(description = "表ID", example = "user_table") @RequestParam @NotBlank String tableId,
            @Parameter(description = "应用名称", example = "user-service") @RequestParam @NotBlank String applicationName) {
        try {
            boolean exists = tableApplicationService.existsTableApplicationRelation(tableId, applicationName);
            logger.debug("检查表应用关联是否存在: tableId={}, applicationName={}, 结果={}", 
                        tableId, applicationName, exists);
            return ApiResponse.success(exists);
        } catch (Exception e) {
            logger.error("检查表应用关联是否存在异常: tableId={}, applicationName={}", 
                        tableId, applicationName, e);
            return ApiResponse.internalError("检查表应用关联是否存在异常: " + e.getMessage());
        }
    }

    /**
     * 获取表应用关联统计信息
     */
    @Operation(summary = "获取表应用关联统计信息", description = "获取表应用关联的统计信息")
    @GetMapping("/stats")
    public ApiResponse<Object> getTableApplicationStats() {
        try {
            Object stats = tableApplicationService.getTableApplicationStats();
            logger.info("获取表应用关联统计信息成功");
            return ApiResponse.success(stats);
        } catch (Exception e) {
            logger.error("获取表应用关联统计信息失败", e);
            return ApiResponse.internalError("获取表应用关联统计信息失败: " + e.getMessage());
        }
    }

    /**
     * 获取支持的表类型列表
     */
    @Operation(summary = "获取支持的表类型列表", description = "获取系统支持的所有表类型")
    @GetMapping("/supported-table-types")
    public ApiResponse<List<String>> getSupportedTableTypes() {
        try {
            List<String> types = tableApplicationService.getSupportedTableTypes();
            logger.info("获取支持的表类型列表成功，数量: {}", types.size());
            return ApiResponse.success(types);
        } catch (Exception e) {
            logger.error("获取支持的表类型列表失败", e);
            return ApiResponse.internalError("获取支持的表类型列表失败: " + e.getMessage());
        }
    }
} 