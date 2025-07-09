package cn.com.multi_writer.service.impl;

import cn.com.multi_writer.service.MetaTableService;
import cn.com.multi_writer.service.dto.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

/**
 * MySQL版本的Meta Hudi Table服务实现类
 * 使用HikariCP连接池管理数据库连接，实现所有MetaTableService接口定义的方法
 */
@Service("mySQLMetaTableService")
public class MySQLMetaTableServiceImpl implements MetaTableService {

    private static final Logger logger = LoggerFactory.getLogger(MySQLMetaTableServiceImpl.class);

    // 通过依赖注入获取数据源
    @Autowired
    private DataSource dataSource;

    private ObjectMapper objectMapper;

    // 数据库表名常量 - 用于存储Hudi元数据表信息
    private static final String HOODIE_META_TABLE = "hoodie_meta_table";

    // 支持的数据库类型
    private static final List<String> SUPPORTED_DB_TYPES = Arrays.asList("MySQL", "PostgreSQL", "Oracle", "SQL Server", "TDSQL", "ClickHouse", "TiDB");

    @PostConstruct
    public void init() {
        try {
            logger.info("初始化MySQL元数据表服务...");

            // 初始化Jackson对象映射器
            this.objectMapper = new ObjectMapper();

            // 测试数据源连接
            try (Connection connection = dataSource.getConnection()) {
                if (connection != null && !connection.isClosed()) {
                    logger.info("数据源连接测试成功，数据库产品: {}", connection.getMetaData().getDatabaseProductName());
                } else {
                    throw new RuntimeException("数据源连接测试失败");
                }
            }

            logger.info("MySQL元数据表服务初始化完成");

        } catch (Exception e) {
            logger.error("初始化MySQL元数据表服务失败", e);
            throw new RuntimeException("初始化MySQL元数据表服务失败: " + e.getMessage(), e);
        }
    }


    /**
     * 获取数据库连接
     *
     * @return 数据库连接
     * @throws SQLException 获取连接失败
     */
    private Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }


    @Override
    public PageResult<MetaTableDTO> getAllTables(int page, int size) {
        try {
            logger.info("获取所有表，页码: {}, 每页大小: {}", page, size);

            // 查询总数
            String countSql = "SELECT COUNT(*) FROM `" + HOODIE_META_TABLE + "` WHERE `cdc_delete_flag` = 0";
            long total = 0;
            try (Connection connection = getConnection(); PreparedStatement pstmt = connection.prepareStatement(countSql)) {
                ResultSet rs = pstmt.executeQuery();
                if (rs.next()) {
                    total = rs.getLong(1);
                }
            }

            // 分页查询数据
            String sql = "SELECT `id`, `schema`, `status`, `is_partitioned`, `partition_expr`, " + "`hoodie_config`, `tags`, `description`, `source_db`, `source_table`, " + "`db_type`, `create_time`, `update_time`, `cdc_delete_flag` " + "FROM `" + HOODIE_META_TABLE + "` " + "WHERE `cdc_delete_flag` = 0 " + "ORDER BY `update_time` DESC " + "LIMIT ? OFFSET ?";

            List<MetaTableDTO> tables = new ArrayList<>();
            try (Connection connection = getConnection(); PreparedStatement pstmt = connection.prepareStatement(sql)) {
                pstmt.setInt(1, size);
                pstmt.setInt(2, page * size);
                ResultSet rs = pstmt.executeQuery();

                while (rs.next()) {
                    tables.add(mapResultSetToDTO(rs));
                }
            }

            logger.info("获取所有表完成，总数: {}, 当前页数据: {}", total, tables.size());
            return new PageResult<>(tables, total, page, size);

        } catch (SQLException e) {
            logger.error("获取所有表失败", e);
            throw new RuntimeException("获取所有表失败: " + e.getMessage(), e);
        }
    }

    @Override
    public Optional<MetaTableDTO> getTableById(String id) {
        try {
            logger.debug("根据ID获取表详情: {}", id);

            String sql = "SELECT `id`, `schema`, `status`, `is_partitioned`, `partition_expr`, " + "`hoodie_config`, `tags`, `description`, `source_db`, `source_table`, " + "`db_type`, `create_time`, `update_time`, `cdc_delete_flag` " + "FROM `" + HOODIE_META_TABLE + "` " + "WHERE `id` = ? AND `cdc_delete_flag` = 0";

            try (Connection connection = getConnection(); PreparedStatement pstmt = connection.prepareStatement(sql)) {
                pstmt.setString(1, id);
                ResultSet rs = pstmt.executeQuery();

                if (rs.next()) {
                    MetaTableDTO dto = mapResultSetToDTO(rs);
                    logger.debug("找到表: {}", id);
                    return Optional.of(dto);
                }
            }

            logger.debug("未找到表: {}", id);
            return Optional.empty();

        } catch (SQLException e) {
            logger.error("根据ID获取表详情失败: {}", id, e);
            throw new RuntimeException("根据ID获取表详情失败: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean createTable(CreateTableRequest request) {
        try {
            logger.info("创建新表: {}", request.getId());

            // 检查表ID是否已存在
            if (existsTableId(request.getId())) {
                throw new IllegalArgumentException("表ID " + request.getId() + " 已存在");
            }

            // 验证schema
            if (!validateTableSchema(request.getSchema())) {
                throw new IllegalArgumentException("Schema格式无效");
            }

            String sql = "INSERT INTO `" + HOODIE_META_TABLE + "` " + "(`id`, `schema`, `status`, `is_partitioned`, `partition_expr`, `hoodie_config`, " + "`tags`, `description`, `source_db`, `source_table`, `db_type`, `cdc_delete_flag`) " + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            try (Connection connection = getConnection(); PreparedStatement pstmt = connection.prepareStatement(sql)) {
                pstmt.setString(1, request.getId());
                pstmt.setString(2, request.getSchema());
                pstmt.setInt(3, 0); // 默认未上线
                pstmt.setBoolean(4, request.getIsPartitioned());
                pstmt.setString(5, request.getPartitionExpr());
                pstmt.setString(6, request.getHoodieConfig());
                pstmt.setString(7, request.getTags());
                pstmt.setString(8, request.getDescription());
                pstmt.setString(9, request.getSourceDb());
                pstmt.setString(10, request.getSourceTable());
                pstmt.setString(11, request.getDbType());
                pstmt.setInt(12, 0); // 未删除

                int result = pstmt.executeUpdate();
                if (result > 0) {
                    logger.info("成功创建表: {}", request.getId());
                    return true;
                } else {
                    logger.error("创建表失败: {}", request.getId());
                    return false;
                }
            }

        } catch (SQLException e) {
            logger.error("创建表异常: {}", request.getId(), e);
            throw new RuntimeException("创建表失败: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean updateTable(String id, UpdateTableRequest request) {
        try {
            logger.info("更新表信息: {}", id);

            // 检查表是否存在
            if (!existsTableId(id)) {
                throw new IllegalArgumentException("表ID " + id + " 不存在");
            }

            // 验证schema（如果提供）
            if (request.getSchema() != null && !validateTableSchema(request.getSchema())) {
                throw new IllegalArgumentException("Schema格式无效");
            }

            // 构建动态更新SQL
            List<String> updateFields = new ArrayList<>();
            List<Object> params = new ArrayList<>();

            if (request.getSchema() != null) {
                updateFields.add("`schema` = ?");
                params.add(request.getSchema());
            }
            if (request.getStatus() != null) {
                updateFields.add("`status` = ?");
                params.add(request.getStatus());
            }
            if (request.getIsPartitioned() != null) {
                updateFields.add("`is_partitioned` = ?");
                params.add(request.getIsPartitioned());
            }
            if (request.getPartitionExpr() != null) {
                updateFields.add("`partition_expr` = ?");
                params.add(request.getPartitionExpr());
            }
            if (request.getHoodieConfig() != null) {
                updateFields.add("`hoodie_config` = ?");
                params.add(request.getHoodieConfig());
            }
            if (request.getTags() != null) {
                updateFields.add("`tags` = ?");
                params.add(request.getTags());
            }
            if (request.getDescription() != null) {
                updateFields.add("`description` = ?");
                params.add(request.getDescription());
            }
            if (request.getSourceDb() != null) {
                updateFields.add("`source_db` = ?");
                params.add(request.getSourceDb());
            }
            if (request.getSourceTable() != null) {
                updateFields.add("`source_table` = ?");
                params.add(request.getSourceTable());
            }
            if (request.getDbType() != null) {
                updateFields.add("`db_type` = ?");
                params.add(request.getDbType());
            }

            if (updateFields.isEmpty()) {
                logger.warn("更新表信息时没有提供任何字段: {}", id);
                return true;
            }

            updateFields.add("`update_time` = CURRENT_TIMESTAMP");
            params.add(id);

            String sql = "UPDATE `" + HOODIE_META_TABLE + "` SET " + String.join(", ", updateFields) + " WHERE `id` = ? AND `cdc_delete_flag` = 0";

            try (Connection connection = getConnection(); PreparedStatement pstmt = connection.prepareStatement(sql)) {
                for (int i = 0; i < params.size(); i++) {
                    pstmt.setObject(i + 1, params.get(i));
                }

                int result = pstmt.executeUpdate();
                if (result > 0) {
                    logger.info("成功更新表: {}", id);
                    return true;
                } else {
                    logger.error("更新表失败: {}", id);
                    return false;
                }
            }

        } catch (SQLException e) {
            logger.error("更新表异常: {}", id, e);
            throw new RuntimeException("更新表失败: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean deleteTable(String id) {
        try {
            logger.info("删除表: {}", id);

            // 逻辑删除，设置cdc_delete_flag为1
            String sql = "UPDATE `" + HOODIE_META_TABLE + "` SET `cdc_delete_flag` = 1, `update_time` = CURRENT_TIMESTAMP WHERE `id` = ? AND `cdc_delete_flag` = 0";

            try (Connection connection = getConnection(); PreparedStatement pstmt = connection.prepareStatement(sql)) {
                pstmt.setString(1, id);
                int result = pstmt.executeUpdate();

                if (result > 0) {
                    logger.info("成功删除表: {}", id);
                    return true;
                } else {
                    logger.error("删除表失败，表不存在: {}", id);
                    return false;
                }
            }

        } catch (SQLException e) {
            logger.error("删除表异常: {}", id, e);
            throw new RuntimeException("删除表失败: " + e.getMessage(), e);
        }
    }

    @Override
    public PageResult<MetaTableDTO> searchTables(SearchCriteria criteria, int page, int size) {
        try {
            logger.info("搜索表，条件: {}", criteria);

            // 构建查询条件
            List<String> conditions = new ArrayList<>();
            List<Object> params = new ArrayList<>();

            conditions.add("`cdc_delete_flag` = 0");

            if (criteria.getKeyword() != null && !criteria.getKeyword().trim().isEmpty()) {
                String keyword = "%" + criteria.getKeyword().trim() + "%";
                conditions.add("(`id` LIKE ? OR `description` LIKE ? OR `source_table` LIKE ?)");
                params.add(keyword);
                params.add(keyword);
                params.add(keyword);
            }

            if (criteria.getStatus() != null) {
                conditions.add("`status` = ?");
                params.add(criteria.getStatus());
            }

            if (criteria.getSourceDb() != null && !criteria.getSourceDb().trim().isEmpty()) {
                conditions.add("`source_db` = ?");
                params.add(criteria.getSourceDb());
            }

            if (criteria.getDbType() != null && !criteria.getDbType().trim().isEmpty()) {
                conditions.add("`db_type` = ?");
                params.add(criteria.getDbType());
            }

            if (criteria.getIsPartitioned() != null) {
                conditions.add("`is_partitioned` = ?");
                params.add(criteria.getIsPartitioned());
            }

            if (criteria.getCreateTimeStart() != null) {
                conditions.add("`create_time` >= ?");
                params.add(Timestamp.valueOf(criteria.getCreateTimeStart()));
            }

            if (criteria.getCreateTimeEnd() != null) {
                conditions.add("`create_time` <= ?");
                params.add(Timestamp.valueOf(criteria.getCreateTimeEnd()));
            }

            String whereClause = String.join(" AND ", conditions);

            // 查询总数
            String countSql = "SELECT COUNT(*) FROM `" + HOODIE_META_TABLE + "` WHERE " + whereClause;
            long total = 0;
            try (Connection connection = getConnection(); PreparedStatement pstmt = connection.prepareStatement(countSql)) {
                for (int i = 0; i < params.size(); i++) {
                    pstmt.setObject(i + 1, params.get(i));
                }
                ResultSet rs = pstmt.executeQuery();
                if (rs.next()) {
                    total = rs.getLong(1);
                }
            }

            // 分页查询数据
            String sql = "SELECT `id`, `schema`, `status`, `is_partitioned`, `partition_expr`, " + "`hoodie_config`, `tags`, `description`, `source_db`, `source_table`, " + "`db_type`, `create_time`, `update_time`, `cdc_delete_flag` " + "FROM `" + HOODIE_META_TABLE + "` " + "WHERE " + whereClause + " " + "ORDER BY `update_time` DESC " + "LIMIT ? OFFSET ?";

            List<MetaTableDTO> tables = new ArrayList<>();
            try (Connection connection = getConnection(); PreparedStatement pstmt = connection.prepareStatement(sql)) {
                for (int i = 0; i < params.size(); i++) {
                    pstmt.setObject(i + 1, params.get(i));
                }
                pstmt.setInt(params.size() + 1, size);
                pstmt.setInt(params.size() + 2, page * size);

                ResultSet rs = pstmt.executeQuery();
                while (rs.next()) {
                    tables.add(mapResultSetToDTO(rs));
                }
            }

            logger.info("搜索完成，找到 {} 条记录", total);
            return new PageResult<>(tables, total, page, size);

        } catch (SQLException e) {
            logger.error("搜索表失败", e);
            throw new RuntimeException("搜索表失败: " + e.getMessage(), e);
        }
    }

    @Override
    public BatchOperationResult batchOperation(BatchOperationRequest request) {
        logger.info("批量操作: {}, 影响表数: {}", request.getOperation(), request.getIds().size());

        switch (request.getOperation().toLowerCase()) {
            case "delete":
                return batchDeleteTables(request.getIds());
            case "online":
                return batchOnlineTables(request.getIds());
            case "offline":
                return batchOfflineTables(request.getIds());
            case "export":
                return exportTables(request.getIds());
            default:
                throw new IllegalArgumentException("不支持的批量操作类型: " + request.getOperation());
        }
    }

    @Override
    public TableStatusStats getTableStatusStats() {
        try {
            logger.info("获取表状态统计");

            String sql = "SELECT " + "COUNT(*) as total, " + "SUM(CASE WHEN `status` = 1 THEN 1 ELSE 0 END) as online, " + "SUM(CASE WHEN `status` = 0 THEN 1 ELSE 0 END) as offline, " + "SUM(CASE WHEN `is_partitioned` = 1 THEN 1 ELSE 0 END) as partitioned, " + "SUM(CASE WHEN `is_partitioned` = 0 THEN 1 ELSE 0 END) as non_partitioned " + "FROM `" + HOODIE_META_TABLE + "` " + "WHERE `cdc_delete_flag` = 0";

            try (Connection connection = getConnection(); PreparedStatement pstmt = connection.prepareStatement(sql)) {
                ResultSet rs = pstmt.executeQuery();
                if (rs.next()) {
                    long total = rs.getLong("total");
                    long online = rs.getLong("online");
                    long offline = rs.getLong("offline");
                    long partitioned = rs.getLong("partitioned");
                    long nonPartitioned = rs.getLong("non_partitioned");

                    return new TableStatusStats(total, online, offline, partitioned, nonPartitioned);
                }
            }

            return new TableStatusStats(0, 0, 0, 0, 0);

        } catch (SQLException e) {
            logger.error("获取表状态统计失败", e);
            throw new RuntimeException("获取表状态统计失败: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean onlineTable(String id) {
        return updateTableStatus(id, 1);
    }

    @Override
    public boolean offlineTable(String id) {
        return updateTableStatus(id, 0);
    }

    @Override
    public boolean validateTableSchema(String schema) {
        try {
            logger.debug("验证表schema: {}", schema.substring(0, Math.min(100, schema.length())));

            // 使用Jackson验证JSON格式
            objectMapper.readTree(schema);

            // 可以添加更多的schema验证逻辑
            return true;

        } catch (Exception e) {
            logger.error("Schema验证失败", e);
            return false;
        }
    }

    @Override
    public List<String> getSupportedDbTypes() {
        return new ArrayList<>(SUPPORTED_DB_TYPES);
    }

    @Override
    public boolean existsTableId(String id) {
        try {
            String sql = "SELECT 1 FROM `" + HOODIE_META_TABLE + "` WHERE `id` = ? AND `cdc_delete_flag` = 0";
            try (Connection connection = getConnection(); PreparedStatement pstmt = connection.prepareStatement(sql)) {
                pstmt.setString(1, id);
                ResultSet rs = pstmt.executeQuery();
                return rs.next();
            }
        } catch (SQLException e) {
            logger.error("检查表ID是否存在失败: {}", id, e);
            return false;
        }
    }

    @Override
    public BatchOperationResult batchDeleteTables(List<String> ids) {
        return batchUpdateOperation(ids, "DELETE", "UPDATE `" + HOODIE_META_TABLE + "` SET `cdc_delete_flag` = 1, `update_time` = CURRENT_TIMESTAMP WHERE `id` = ? AND `cdc_delete_flag` = 0");
    }

    @Override
    public BatchOperationResult batchOnlineTables(List<String> ids) {
        return batchUpdateOperation(ids, "ONLINE", "UPDATE `" + HOODIE_META_TABLE + "` SET `status` = 1, `update_time` = CURRENT_TIMESTAMP WHERE `id` = ? AND `cdc_delete_flag` = 0");
    }

    @Override
    public BatchOperationResult batchOfflineTables(List<String> ids) {
        return batchUpdateOperation(ids, "OFFLINE", "UPDATE `" + HOODIE_META_TABLE + "` SET `status` = 0, `update_time` = CURRENT_TIMESTAMP WHERE `id` = ? AND `cdc_delete_flag` = 0");
    }

    @Override
    public BatchOperationResult exportTables(List<String> ids) {
        // 导出功能的具体实现
        logger.info("导出表: {}", ids);
        // 这里可以实现具体的导出逻辑，比如导出为JSON、CSV等格式
        return new BatchOperationResult(ids.size(), 0, ids.size(), new ArrayList<>());
    }

    @Override
    public BatchOperationResult exportSearchResults(SearchCriteria criteria) {
        try {
            PageResult<MetaTableDTO> result = searchTables(criteria, 0, Integer.MAX_VALUE);
            List<String> ids = result.getData().stream().map(MetaTableDTO::getId).collect(Collectors.toList());
            return exportTables(ids);
        } catch (Exception e) {
            logger.error("导出搜索结果失败", e);
            throw new RuntimeException("导出搜索结果失败: " + e.getMessage(), e);
        }
    }

    @Override
    public Optional<String> getTableSchema(String id) {
        Optional<MetaTableDTO> table = getTableById(id);
        return table.map(MetaTableDTO::getSchema);
    }

    @Override
    public List<Object> getTableHistory(String id) {
        // 获取表历史记录的具体实现
        logger.info("获取表历史记录: {}", id);
        // 这里可以实现具体的历史记录查询逻辑
        // 由于当前表结构没有历史记录字段，返回空列表
        return new ArrayList<>();
    }

    @Override
    public boolean copyTable(String id, String newId) {
        try {
            Optional<MetaTableDTO> sourceTable = getTableById(id);
            if (!sourceTable.isPresent()) {
                throw new IllegalArgumentException("源表不存在: " + id);
            }

            if (existsTableId(newId)) {
                throw new IllegalArgumentException("目标表ID已存在: " + newId);
            }

            MetaTableDTO source = sourceTable.get();
            CreateTableRequest request = new CreateTableRequest();
            request.setId(newId);
            request.setSchema(source.getSchema());
            request.setIsPartitioned(source.getIsPartitioned());
            request.setPartitionExpr(source.getPartitionExpr());
            request.setTags(source.getTags());
            request.setDescription("复制自: " + id + " - " + source.getDescription());
            request.setSourceDb(source.getSourceDb());
            request.setSourceTable(source.getSourceTable());
            request.setDbType(source.getDbType());
            request.setHoodieConfig(source.getHoodieConfig());

            return createTable(request);

        } catch (Exception e) {
            logger.error("复制表失败: {} -> {}", id, newId, e);
            throw new RuntimeException("复制表失败: " + e.getMessage(), e);
        }
    }

    @Override
    public Object getTableUsageStats(String id) {
        // 获取表使用统计的具体实现
        logger.info("获取表使用统计: {}", id);
        Map<String, Object> stats = new HashMap<>();
        stats.put("tableId", id);
        stats.put("lastAccessTime", LocalDateTime.now());
        stats.put("accessCount", 0);
        return stats;
    }

    @Override
    public PageResult<MetaTableDTO> getDeletedTables(int page, int size) {
        try {
            logger.info("获取已删除表，页码: {}, 每页大小: {}", page, size);

            // 查询总数
            String countSql = "SELECT COUNT(*) FROM `" + HOODIE_META_TABLE + "` WHERE `cdc_delete_flag` = 1";
            long total = 0;
            try (Connection connection = getConnection(); PreparedStatement pstmt = connection.prepareStatement(countSql)) {
                ResultSet rs = pstmt.executeQuery();
                if (rs.next()) {
                    total = rs.getLong(1);
                }
            }

            // 分页查询数据
            String sql = "SELECT `id`, `schema`, `status`, `is_partitioned`, `partition_expr`, " + "`hoodie_config`, `tags`, `description`, `source_db`, `source_table`, " + "`db_type`, `create_time`, `update_time`, `cdc_delete_flag` " + "FROM `" + HOODIE_META_TABLE + "` " + "WHERE `cdc_delete_flag` = 1 " + "ORDER BY `update_time` DESC " + "LIMIT ? OFFSET ?";

            List<MetaTableDTO> tables = new ArrayList<>();
            try (Connection connection = getConnection(); PreparedStatement pstmt = connection.prepareStatement(sql)) {
                pstmt.setInt(1, size);
                pstmt.setInt(2, page * size);
                ResultSet rs = pstmt.executeQuery();

                while (rs.next()) {
                    tables.add(mapResultSetToDTO(rs));
                }
            }

            logger.info("获取已删除表完成，总数: {}, 当前页数据: {}", total, tables.size());
            return new PageResult<>(tables, total, page, size);

        } catch (SQLException e) {
            logger.error("获取已删除表失败", e);
            throw new RuntimeException("获取已删除表失败: " + e.getMessage(), e);
        }
    }

    /**
     * 批量更新操作的通用方法
     */
    private BatchOperationResult batchUpdateOperation(List<String> ids, String operationType, String sql) {
        List<String> errors = new ArrayList<>();
        int success = 0;
        int failed = 0;

        for (String id : ids) {
            try {
                try (Connection connection = getConnection(); PreparedStatement pstmt = connection.prepareStatement(sql)) {
                    pstmt.setString(1, id);
                    int result = pstmt.executeUpdate();
                    if (result > 0) {
                        success++;
                    } else {
                        failed++;
                        errors.add(operationType + "失败: " + id);
                    }
                }
            } catch (SQLException e) {
                failed++;
                errors.add(operationType + "异常: " + id + " - " + e.getMessage());
            }
        }

        return new BatchOperationResult(success, failed, ids.size(), errors);
    }

    /**
     * 更新表状态的通用方法
     */
    private boolean updateTableStatus(String id, int status) {
        try {
            logger.info("更新表状态: {} -> {}", id, status);

            String sql = "UPDATE `" + HOODIE_META_TABLE + "` SET `status` = ?, `update_time` = CURRENT_TIMESTAMP WHERE `id` = ? AND `cdc_delete_flag` = 0";
            try (Connection connection = getConnection(); PreparedStatement pstmt = connection.prepareStatement(sql)) {
                pstmt.setInt(1, status);
                pstmt.setString(2, id);

                int result = pstmt.executeUpdate();
                if (result > 0) {
                    logger.info("成功更新表状态: {}", id);
                    return true;
                } else {
                    logger.error("更新表状态失败，表不存在: {}", id);
                    return false;
                }
            }

        } catch (SQLException e) {
            logger.error("更新表状态异常: {}", id, e);
            throw new RuntimeException("更新表状态失败: " + e.getMessage(), e);
        }
    }

    /**
     * 将ResultSet映射为MetaTableDTO
     */
    private MetaTableDTO mapResultSetToDTO(ResultSet rs) throws SQLException {
        MetaTableDTO dto = new MetaTableDTO();
        dto.setId(rs.getString("id"));
        dto.setSchema(rs.getString("schema"));
        dto.setStatus(rs.getInt("status"));
        dto.setIsPartitioned(rs.getBoolean("is_partitioned"));
        dto.setPartitionExpr(rs.getString("partition_expr"));
        dto.setHoodieConfig(rs.getString("hoodie_config"));
        dto.setTags(rs.getString("tags"));
        dto.setDescription(rs.getString("description"));
        dto.setSourceDb(rs.getString("source_db"));
        dto.setSourceTable(rs.getString("source_table"));
        dto.setDbType(rs.getString("db_type"));
        dto.setCdcDeleteFlag(rs.getInt("cdc_delete_flag"));

        // 转换时间戳为LocalDateTime
        Timestamp createTime = rs.getTimestamp("create_time");
        if (createTime != null) {
            dto.setCreateTime(createTime.toLocalDateTime());
        }

        Timestamp updateTime = rs.getTimestamp("update_time");
        if (updateTime != null) {
            dto.setUpdateTime(updateTime.toLocalDateTime());
        }

        return dto;
    }

    @Override
    public List<Object> getSystemTables() {
        // 获取mysql数据库下的所有表
        try (Connection connection = getConnection(); Statement stmt = connection.createStatement()) {
            // 获取数据库下的所有表的具体信息
            ResultSet rs = stmt.executeQuery("SHOW TABLE STATUS");
            List<Object> tables = new ArrayList<>();
            while (rs.next()) {
                tables.add(rs.getString("Name"));
            }
            return tables;
        }
        catch (SQLException e) {
            logger.error("获取系统表失败", e);
            throw new RuntimeException("获取系统表失败: " + e.getMessage(), e);
        }
    }

    @Override
    public Object getSystemTableStats() {
        // 获取mysql数据库下的所有表的统计信息
        try (Connection connection = getConnection(); Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery("SHOW TABLE STATUS");
            List<Object> tables = new ArrayList<>();
            Map<String, Object> stats = new HashMap<>();
            while (rs.next()) {
                stats.put(rs.getString("Name"), rs.getString("Rows"));
            }
            return stats;
        } catch (SQLException e) {
            logger.error("获取系统表统计信息失败", e);
            throw new RuntimeException("获取系统表统计信息失败: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean restoreTable(String id) {
        // 恢复表的具体实现
        logger.info("恢复表: {}", id);
       
        try {
            String sql = "UPDATE `" + HOODIE_META_TABLE + "` SET `cdc_delete_flag` = 0, `update_time` = CURRENT_TIMESTAMP WHERE `id` = ?";
            try (Connection connection = getConnection(); PreparedStatement pstmt = connection.prepareStatement(sql)) {
                pstmt.setString(1, id);
                int result = pstmt.executeUpdate();
            }
        } catch (SQLException e) {    
            logger.error("恢复表失败: {}", id, e);  
            throw new RuntimeException("恢复表失败: " + e.getMessage(), e);
        }
        return true;
    }
} 