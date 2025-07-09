package cn.com.multi_writer.web.controller;

import cn.com.multi_writer.service.DbServerService;
import cn.com.multi_writer.service.dto.*;
import cn.com.multi_writer.web.common.ApiResponse;
import cn.com.multi_writer.web.exception.BusinessException;
import cn.com.multi_writer.web.exception.ResourceNotFoundException;
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
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 数据库服务器管理 REST API 控制器
 */
@Tag(name = "数据库服务器管理", description = "提供数据库服务器的CRUD操作和管理功能")
@RestController
@RequestMapping("/api/db-servers")
@Validated
public class DbServerController {

    private static final Logger logger = LoggerFactory.getLogger(DbServerController.class);

    @Autowired
    @Qualifier("mySQLDbServerService")
    private DbServerService dbServerService;

    /**
     * 获取所有数据库服务器（分页）
     */
    @Operation(summary = "获取所有数据库服务器", description = "分页获取所有数据库服务器信息")
    @GetMapping
    public ApiResponse<PageResult<DbServerDTO>> getAllDbServers(
            @Parameter(description = "页码(从0开始)", example = "0") @RequestParam(defaultValue = "0") @PositiveOrZero int page,
            @Parameter(description = "每页大小", example = "10") @RequestParam(defaultValue = "10") @Positive int size) {

        logger.info("获取所有数据库服务器，页码: {}, 每页大小: {}", page, size);

        try {
            PageResult<DbServerDTO> result = dbServerService.getAllDbServers(page, size);
            return ApiResponse.success(result);
        } catch (Exception e) {
            logger.error("获取所有数据库服务器失败", e);
            throw new BusinessException("获取数据库服务器列表失败: " + e.getMessage(), e);
        }
    }

    /**
     * 根据ID获取数据库服务器详情
     */
    @Operation(summary = "根据ID获取数据库服务器详情", description = "根据服务器ID获取详细的数据库服务器信息")
    @GetMapping("/{id}")
    public ApiResponse<DbServerDTO> getDbServerById(@Parameter(description = "服务器ID", example = "1") @PathVariable @Positive Integer id) {
        logger.info("根据ID获取数据库服务器详情: {}", id);

        try {
            Optional<DbServerDTO> server = dbServerService.getDbServerById(id);
            if (!server.isPresent()) {
                throw new ResourceNotFoundException("数据库服务器不存在: " + id);
            }
            return ApiResponse.success(server.get());
        } catch (ResourceNotFoundException e) {
            throw e;
        } catch (Exception e) {
            logger.error("根据ID获取数据库服务器详情失败: {}", id, e);
            throw new BusinessException("获取数据库服务器详情失败: " + e.getMessage(), e);
        }
    }

    /**
     * 创建新的数据库服务器
     */
    @Operation(summary = "创建新的数据库服务器", description = "创建新的数据库服务器")
    @PostMapping
    public ApiResponse<String> createDbServer(@Valid @RequestBody CreateDbServerRequest request) {
        logger.info("创建新的数据库服务器: {}", request.getName());

        try {
            boolean success = dbServerService.createDbServer(request);
            if (success) {
                return ApiResponse.created("数据库服务器创建成功: " + request.getName());
            } else {
                throw new BusinessException("数据库服务器创建失败");
            }
        } catch (IllegalArgumentException e) {
            throw new BusinessException(400, e.getMessage(), e);
        } catch (Exception e) {
            logger.error("创建数据库服务器失败: {}", request.getName(), e);
            throw new BusinessException("创建数据库服务器失败: " + e.getMessage(), e);
        }
    }

    /**
     * 更新数据库服务器信息
     */
    @Operation(summary = "更新数据库服务器信息", description = "更新指定数据库服务器的信息")
    @PutMapping("/{id}")
    public ApiResponse<String> updateDbServer(@Parameter(description = "服务器ID", example = "1") @PathVariable @Positive Integer id,
                                              @Valid @RequestBody UpdateDbServerRequest request) {
        logger.info("更新数据库服务器信息: {}", id);

        try {
            boolean success = dbServerService.updateDbServer(id, request);
            if (success) {
                return ApiResponse.success("数据库服务器更新成功: " + id);
            } else {
                throw new BusinessException("数据库服务器更新失败");
            }
        } catch (IllegalArgumentException e) {
            throw new ResourceNotFoundException(e.getMessage());
        } catch (Exception e) {
            logger.error("更新数据库服务器失败: {}", id, e);
            throw new BusinessException("更新数据库服务器失败: " + e.getMessage(), e);
        }
    }

    /**
     * 删除数据库服务器
     */
    @Operation(summary = "删除数据库服务器", description = "删除指定的数据库服务器")
    @DeleteMapping("/{id}")
    public ApiResponse<String> deleteDbServer(@Parameter(description = "服务器ID", example = "1") @PathVariable @Positive Integer id) {
        logger.info("删除数据库服务器: {}", id);

        try {
            boolean success = dbServerService.deleteDbServer(id);
            if (success) {
                return ApiResponse.success("数据库服务器删除成功: " + id);
            } else {
                throw new BusinessException("数据库服务器删除失败");
            }
        } catch (Exception e) {
            logger.error("删除数据库服务器失败: {}", id, e);
            throw new BusinessException("删除数据库服务器失败: " + e.getMessage(), e);
        }
    }

    /**
     * 搜索数据库服务器
     */
    @Operation(summary = "搜索数据库服务器", description = "根据条件搜索数据库服务器")
    @PostMapping("/search")
    public ApiResponse<PageResult<DbServerDTO>> searchDbServers(
            @Valid @RequestBody DbServerSearchCriteria criteria,
            @Parameter(description = "页码(从0开始)", example = "0") @RequestParam(defaultValue = "0") @PositiveOrZero int page,
            @Parameter(description = "每页大小", example = "10") @RequestParam(defaultValue = "10") @Positive int size) {

        logger.info("搜索数据库服务器，页码: {}, 每页大小: {}", page, size);

        try {
            PageResult<DbServerDTO> result = dbServerService.searchDbServers(criteria, page, size);
            return ApiResponse.success(result);
        } catch (Exception e) {
            logger.error("搜索数据库服务器失败", e);
            throw new BusinessException("搜索数据库服务器失败: " + e.getMessage(), e);
        }
    }

    /**
     * 批量删除数据库服务器
     */
    @Operation(summary = "批量删除数据库服务器", description = "批量删除指定的数据库服务器")
    @DeleteMapping("/batch")
    public ApiResponse<BatchOperationResult> batchDeleteDbServers(@RequestBody @NotEmpty List<Integer> ids) {
        logger.info("批量删除数据库服务器，数量: {}", ids.size());

        try {
            BatchOperationResult result = dbServerService.batchDeleteDbServers(ids);
            return ApiResponse.success(result);
        } catch (Exception e) {
            logger.error("批量删除数据库服务器失败", e);
            throw new BusinessException("批量删除数据库服务器失败: " + e.getMessage(), e);
        }
    }

    /**
     * 检查数据库服务器ID是否存在
     */
    @Operation(summary = "检查数据库服务器ID是否存在", description = "检查指定ID的数据库服务器是否存在")
    @GetMapping("/{id}/exists")
    public ApiResponse<Boolean> existsDbServerId(@Parameter(description = "服务器ID", example = "1") @PathVariable @Positive Integer id) {
        logger.info("检查数据库服务器ID是否存在: {}", id);

        try {
            boolean exists = dbServerService.existsDbServerId(id);
            return ApiResponse.success(exists);
        } catch (Exception e) {
            logger.error("检查数据库服务器ID存在性失败: {}", id, e);
            throw new BusinessException("检查数据库服务器ID存在性失败: " + e.getMessage(), e);
        }
    }

    /**
     * 获取支持的数据库类型列表
     */
    @Operation(summary = "获取支持的数据库类型列表", description = "获取系统支持的所有数据库类型")
    @GetMapping("/supported-types")
    public ApiResponse<List<String>> getSupportedSourceTypes() {
        logger.info("获取支持的数据库类型列表");

        try {
            List<String> types = dbServerService.getSupportedSourceTypes();
            return ApiResponse.success(types);
        } catch (Exception e) {
            logger.error("获取支持的数据库类型列表失败", e);
            throw new BusinessException("获取支持的数据库类型列表失败: " + e.getMessage(), e);
        }
    }

    /**
     * 测试数据库连接
     */
    @Operation(summary = "测试数据库连接", description = "测试指定数据库服务器的连接状态")
    @PostMapping("/{id}/test")
    public ApiResponse<Boolean> testConnection(@Parameter(description = "服务器ID", example = "1") @PathVariable @Positive Integer id) {
        logger.info("测试数据库连接: {}", id);

        try {
            boolean connected = dbServerService.testDbConnection(id);
            return ApiResponse.success(connected);
        } catch (Exception e) {
            logger.error("测试数据库连接失败: {}", id, e);
            throw new BusinessException("测试数据库连接失败: " + e.getMessage(), e);
        }
    }

    /**
     * 获取数据库列表
     */
    @Operation(summary = "获取数据库列表", description = "获取指定数据库服务器中的所有数据库列表")
    @GetMapping("/{id}/databases")
    public ApiResponse<List<String>> getDatabases(
            @Parameter(description = "服务器ID", example = "1") @PathVariable @Positive Integer id) throws SQLException {
        logger.info("获取数据库列表: {}", id);
        return ApiResponse.success(dbServerService.getDatabases(id));
    }

    /**
     * 获取业务表列表
     */
    @Operation(summary = "获取业务表列表", description = "获取指定数据库中的业务表列表")
    @GetMapping("/{id}/business-tables")
    public ApiResponse<List<BusinessTableInfoDTO>> getBusinessTables(
            @Parameter(description = "服务器ID", example = "1") @PathVariable @Positive Integer id,
            @Parameter(description = "数据库名", example = "test_db") @RequestParam(required = true) String database) throws SQLException {
        logger.info("获取业务表列表: serverId={}, database={}", id, database);
        return ApiResponse.success(dbServerService.getBusinessTables(id, database));
    }

    /**
     * 获取表结构信息
     */
    @Operation(summary = "获取表结构信息", description = "获取指定数据库表的详细结构信息，包括字段信息和主键信息")
    @GetMapping("/{id}/table-schema")
    public ApiResponse<TableSchemaInfoDTO> getTableSchema(
            @Parameter(description = "服务器ID", example = "1") @PathVariable @Positive Integer id,
            @Parameter(description = "数据库名", example = "test_db") @RequestParam(required = true) String database,
            @Parameter(description = "表名", example = "user_table") @RequestParam(required = true) String tableName) throws SQLException {
        logger.info("获取表结构: serverId={}, database={}, tableName={}", id, database, tableName);
        return ApiResponse.success(dbServerService.getTableSchema(id, database, tableName));
    }

    /**
     * 预览表数据
     */
    @Operation(summary = "预览表数据", description = "预览指定数据库表的前几行数据")
    @GetMapping("/{id}/table-preview")
    public ApiResponse<List<Map<String, Object>>> previewTableData(
            @Parameter(description = "服务器ID", example = "1") @PathVariable @Positive Integer id,
            @Parameter(description = "数据库名", example = "test_db") @RequestParam(required = true) String database,
            @Parameter(description = "表名", example = "user_table") @RequestParam(required = true) String tableName,
            @Parameter(description = "限制行数", example = "10") @RequestParam(defaultValue = "10") @Positive int limit) throws SQLException {
        logger.info("预览表数据: serverId={}, database={}, tableName={}, limit={}", id, database, tableName, limit);
        return ApiResponse.success(dbServerService.previewTableData(id, database, tableName, limit));
    }
} 