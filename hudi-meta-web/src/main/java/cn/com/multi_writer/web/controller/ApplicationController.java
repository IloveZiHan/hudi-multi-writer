package cn.com.multi_writer.web.controller;

import cn.com.multi_writer.service.ApplicationService;
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
import javax.validation.constraints.Positive;
import javax.validation.constraints.PositiveOrZero;
import java.util.List;
import java.util.Optional;

/**
 * 应用程序管理控制器
 * 提供应用程序的CRUD操作和管理功能
 */
@Tag(name = "应用程序管理", description = "提供应用程序的CRUD操作和管理功能")
@RestController
@RequestMapping("/api/applications")
@Validated
public class ApplicationController {

    private static final Logger logger = LoggerFactory.getLogger(ApplicationController.class);

    @Autowired
    @Qualifier("mySQLApplicationService")
    private ApplicationService applicationService;

    /**
     * 获取所有应用程序
     */
    @Operation(summary = "获取所有应用程序", description = "分页获取所有应用程序信息")
    @GetMapping
    public ApiResponse<PageResult<ApplicationDTO>> getAllApplications(
            @Parameter(description = "页码(从0开始)", example = "0") @RequestParam(defaultValue = "0") @PositiveOrZero int page,
            @Parameter(description = "每页大小", example = "10") @RequestParam(defaultValue = "10") @Positive int size) {
        
        try {
            PageResult<ApplicationDTO> result = applicationService.getAllApplications(page, size);
            logger.info("获取应用程序列表成功，页码: {}, 每页大小: {}, 总数: {}", page, size, result.getTotal());
            return ApiResponse.success(result);
        } catch (Exception e) {
            logger.error("获取应用程序列表失败", e);
            return ApiResponse.internalError("获取应用程序列表失败: " + e.getMessage());
        }
    }

    /**
     * 根据ID获取应用程序详情
     */
    @Operation(summary = "根据ID获取应用程序详情", description = "根据应用程序ID获取详细的应用程序信息")
    @GetMapping("/{id}")
    public ApiResponse<ApplicationDTO> getApplicationById(@Parameter(description = "应用程序ID", example = "1") @PathVariable @Positive Integer id) {
        try {
            Optional<ApplicationDTO> application = applicationService.getApplicationById(id);
            if (application.isPresent()) {
                logger.info("获取应用程序详情成功: ID={}", id);
                return ApiResponse.success(application.get());
            } else {
                logger.warn("应用程序不存在: ID={}", id);
                return ApiResponse.notFound("应用程序不存在");
            }
        } catch (Exception e) {
            logger.error("获取应用程序详情失败: ID={}", id, e);
            return ApiResponse.internalError("获取应用程序详情失败: " + e.getMessage());
        }
    }

    /**
     * 创建新的应用程序
     */
    @Operation(summary = "创建新的应用程序", description = "创建新的应用程序")
    @PostMapping
    public ApiResponse<String> createApplication(@Valid @RequestBody CreateApplicationRequest request) {
        try {
            // 检查应用程序名称是否已存在
            if (applicationService.existsApplicationName(request.getName())) {
                logger.warn("应用程序名称已存在: {}", request.getName());
                return ApiResponse.conflict("应用程序名称已存在");
            }

            boolean success = applicationService.createApplication(request);
            if (success) {
                logger.info("创建应用程序成功: {}", request.getName());
                return ApiResponse.created("创建应用程序成功", "应用程序创建完成");
            } else {
                logger.error("创建应用程序失败: {}", request.getName());
                return ApiResponse.internalError("创建应用程序失败");
            }
        } catch (Exception e) {
            logger.error("创建应用程序异常: {}", request.getName(), e);
            return ApiResponse.internalError("创建应用程序异常: " + e.getMessage());
        }
    }

    /**
     * 更新应用程序信息
     */
    @Operation(summary = "更新应用程序信息", description = "更新指定应用程序的信息")
    @PutMapping("/{id}")
    public ApiResponse<String> updateApplication(@Parameter(description = "应用程序ID", example = "1") @PathVariable @Positive Integer id,
                                                @Valid @RequestBody UpdateApplicationRequest request) {
        try {
            // 检查应用程序是否存在
            if (!applicationService.existsApplicationId(id)) {
                logger.warn("应用程序不存在: ID={}", id);
                return ApiResponse.notFound("应用程序不存在");
            }

            // 如果要更新名称，检查新名称是否与其他应用程序冲突
            if (request.getName() != null) {
                Optional<ApplicationDTO> existingApp = applicationService.getApplicationByName(request.getName());
                if (existingApp.isPresent() && !existingApp.get().getId().equals(id)) {
                    logger.warn("应用程序名称已被其他应用程序使用: {}", request.getName());
                    return ApiResponse.conflict("应用程序名称已被其他应用程序使用");
                }
            }

            boolean success = applicationService.updateApplication(id, request);
            if (success) {
                logger.info("更新应用程序成功: ID={}", id);
                return ApiResponse.success("更新应用程序成功");
            } else {
                logger.error("更新应用程序失败: ID={}", id);
                return ApiResponse.internalError("更新应用程序失败");
            }
        } catch (Exception e) {
            logger.error("更新应用程序异常: ID={}", id, e);
            return ApiResponse.internalError("更新应用程序异常: " + e.getMessage());
        }
    }

    /**
     * 删除应用程序
     */
    @Operation(summary = "删除应用程序", description = "删除指定的应用程序")
    @DeleteMapping("/{id}")
    public ApiResponse<String> deleteApplication(@Parameter(description = "应用程序ID", example = "1") @PathVariable @Positive Integer id) {
        try {
            // 检查应用程序是否存在
            if (!applicationService.existsApplicationId(id)) {
                logger.warn("应用程序不存在: ID={}", id);
                return ApiResponse.notFound("应用程序不存在");
            }

            boolean success = applicationService.deleteApplication(id);
            if (success) {
                logger.info("删除应用程序成功: ID={}", id);
                return ApiResponse.success("删除应用程序成功");
            } else {
                logger.error("删除应用程序失败: ID={}", id);
                return ApiResponse.internalError("删除应用程序失败");
            }
        } catch (Exception e) {
            logger.error("删除应用程序异常: ID={}", id, e);
            return ApiResponse.internalError("删除应用程序异常: " + e.getMessage());
        }
    }

    /**
     * 搜索应用程序
     */
    @Operation(summary = "搜索应用程序", description = "根据条件搜索应用程序")
    @PostMapping("/search")
    public ApiResponse<PageResult<ApplicationDTO>> searchApplications(
            @Valid @RequestBody ApplicationSearchCriteria criteria,
            @Parameter(description = "页码(从0开始)", example = "0") @RequestParam(defaultValue = "0") @PositiveOrZero int page,
            @Parameter(description = "每页大小", example = "10") @RequestParam(defaultValue = "10") @Positive int size) {
        
        try {
            PageResult<ApplicationDTO> result = applicationService.searchApplications(criteria, page, size);
            logger.info("搜索应用程序成功，页码: {}, 每页大小: {}, 总数: {}", page, size, result.getTotal());
            return ApiResponse.success(result);
        } catch (Exception e) {
            logger.error("搜索应用程序失败", e);
            return ApiResponse.internalError("搜索应用程序失败: " + e.getMessage());
        }
    }

    /**
     * 批量删除应用程序
     */
    @Operation(summary = "批量删除应用程序", description = "批量删除指定的应用程序")
    @DeleteMapping("/batch")
    public ApiResponse<BatchOperationResult> batchDeleteApplications(@RequestBody @NotEmpty List<Integer> ids) {
        try {
            BatchOperationResult result = applicationService.batchDeleteApplications(ids);
            logger.info("批量删除应用程序完成，成功: {}, 失败: {}, 总数: {}", 
                       result.getSuccess(), result.getFailed(), result.getTotal());
            return ApiResponse.success(result);
        } catch (Exception e) {
            logger.error("批量删除应用程序异常", e);
            return ApiResponse.internalError("批量删除应用程序异常: " + e.getMessage());
        }
    }

    /**
     * 检查应用程序ID是否存在
     */
    @Operation(summary = "检查应用程序ID是否存在", description = "检查指定ID的应用程序是否存在")
    @GetMapping("/{id}/exists")
    public ApiResponse<Boolean> existsApplicationId(@Parameter(description = "应用程序ID", example = "1") @PathVariable @Positive Integer id) {
        try {
            boolean exists = applicationService.existsApplicationId(id);
            logger.debug("检查应用程序ID是否存在: ID={}, 结果={}", id, exists);
            return ApiResponse.success(exists);
        } catch (Exception e) {
            logger.error("检查应用程序ID是否存在异常: ID={}", id, e);
            return ApiResponse.internalError("检查应用程序ID是否存在异常: " + e.getMessage());
        }
    }

    /**
     * 检查应用程序名称是否存在
     */
    @Operation(summary = "检查应用程序名称是否存在", description = "检查指定名称的应用程序是否存在")
    @GetMapping("/name/{name}/exists")
    public ApiResponse<Boolean> existsApplicationName(@Parameter(description = "应用程序名称", example = "user-service") @PathVariable String name) {
        try {
            boolean exists = applicationService.existsApplicationName(name);
            logger.debug("检查应用程序名称是否存在: 名称={}, 结果={}", name, exists);
            return ApiResponse.success(exists);
        } catch (Exception e) {
            logger.error("检查应用程序名称是否存在异常: 名称={}", name, e);
            return ApiResponse.internalError("检查应用程序名称是否存在异常: " + e.getMessage());
        }
    }

    /**
     * 根据名称获取应用程序
     */
    @Operation(summary = "根据名称获取应用程序", description = "根据应用程序名称获取应用程序信息")
    @GetMapping("/name/{name}")
    public ApiResponse<ApplicationDTO> getApplicationByName(@Parameter(description = "应用程序名称", example = "user-service") @PathVariable String name) {
        try {
            Optional<ApplicationDTO> application = applicationService.getApplicationByName(name);
            if (application.isPresent()) {
                logger.info("根据名称获取应用程序成功: 名称={}", name);
                return ApiResponse.success(application.get());
            } else {
                logger.warn("应用程序不存在: 名称={}", name);
                return ApiResponse.notFound("应用程序不存在");
            }
        } catch (Exception e) {
            logger.error("根据名称获取应用程序失败: 名称={}", name, e);
            return ApiResponse.internalError("根据名称获取应用程序失败: " + e.getMessage());
        }
    }

    /**
     * 获取应用程序统计信息
     */
    @Operation(summary = "获取应用程序统计信息", description = "获取应用程序的统计信息")
    @GetMapping("/stats")
    public ApiResponse<Object> getApplicationStats() {
        try {
            Object stats = applicationService.getApplicationStats();
            logger.info("获取应用程序统计信息成功");
            return ApiResponse.success(stats);
        } catch (Exception e) {
            logger.error("获取应用程序统计信息失败", e);
            return ApiResponse.internalError("获取应用程序统计信息失败: " + e.getMessage());
        }
    }
} 