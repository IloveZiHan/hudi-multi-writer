package cn.com.multi_writer.service.impl;

import cn.com.multi_writer.service.DbServerService;
import cn.com.multi_writer.service.dto.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.LinkedHashMap;

/**
 * MySQL版本的数据库服务器服务实现类
 */
@Service("mySQLDbServerService")
public class MySQLDbServerServiceImpl implements DbServerService {

    private static final Logger logger = LoggerFactory.getLogger(MySQLDbServerServiceImpl.class);

    @Autowired
    private DataSource dataSource;

    // 数据库表名常量
    private static final String DB_SERVER_TABLE = "hoodie_meta_db_server";

    // 支持的数据库类型
    private static final List<String> SUPPORTED_SOURCE_TYPES = Arrays.asList(
            "mysql", "oracle", "tdsql", "postgresql", "clickhouse", "tidb", "sqlserver"
    );

    /**
     * 获取数据库连接
     */
    private Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    @Override
    public PageResult<DbServerDTO> getAllDbServers(int page, int size) {
        try {
            logger.info("获取所有数据库服务器，页码: {}, 每页大小: {}", page, size);

            // 查询总数
            String countSql = "SELECT COUNT(*) FROM `" + DB_SERVER_TABLE + "`";
            long total = 0;
            try (Connection connection = getConnection();
                 PreparedStatement pstmt = connection.prepareStatement(countSql)) {
                ResultSet rs = pstmt.executeQuery();
                if (rs.next()) {
                    total = rs.getLong(1);
                }
            }

            // 分页查询数据
            String sql = "SELECT `id`, `name`, `host`, `source_type`, `jdbc_url`, `username`, `password`, " +
                    "`alias`, `description`, `create_user`, `create_time`, `update_user`, `update_time` " +
                    "FROM `" + DB_SERVER_TABLE + "` " +
                    "ORDER BY `create_time` DESC " +
                    "LIMIT ? OFFSET ?";

            List<DbServerDTO> servers = new ArrayList<>();
            try (Connection connection = getConnection();
                 PreparedStatement pstmt = connection.prepareStatement(sql)) {
                pstmt.setInt(1, size);
                pstmt.setInt(2, page * size);
                ResultSet rs = pstmt.executeQuery();

                while (rs.next()) {
                    servers.add(mapResultSetToDTO(rs));
                }
            }

            logger.info("获取所有数据库服务器完成，总数: {}, 当前页数据: {}", total, servers.size());
            return new PageResult<>(servers, total, page, size);

        } catch (SQLException e) {
            logger.error("获取所有数据库服务器失败", e);
            throw new RuntimeException("获取所有数据库服务器失败: " + e.getMessage(), e);
        }
    }

    @Override
    public Optional<DbServerDTO> getDbServerById(Integer id) {
        try {
            logger.debug("根据ID获取数据库服务器详情: {}", id);

            String sql = "SELECT `id`, `name`, `host`, `source_type`, `jdbc_url`, `username`, `password`, " +
                    "`alias`, `description`, `create_user`, `create_time`, `update_user`, `update_time` " +
                    "FROM `" + DB_SERVER_TABLE + "` WHERE `id` = ?";

            try (Connection connection = getConnection();
                 PreparedStatement pstmt = connection.prepareStatement(sql)) {
                pstmt.setInt(1, id);
                ResultSet rs = pstmt.executeQuery();

                if (rs.next()) {
                    DbServerDTO dto = mapResultSetToDTO(rs);
                    logger.debug("找到数据库服务器: {}", id);
                    return Optional.of(dto);
                }
            }

            logger.debug("未找到数据库服务器: {}", id);
            return Optional.empty();

        } catch (SQLException e) {
            logger.error("根据ID获取数据库服务器详情失败: {}", id, e);
            throw new RuntimeException("根据ID获取数据库服务器详情失败: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean createDbServer(CreateDbServerRequest request) {
        try {
            logger.info("创建新的数据库服务器: {}", request.getName());

            // 检查名称是否已存在
            if (getDbServerByName(request.getName()).isPresent()) {
                throw new IllegalArgumentException("数据库服务器名称 " + request.getName() + " 已存在");
            }

            // 检查别名是否已存在（如果提供了别名）
            if (request.getAlias() != null && !request.getAlias().trim().isEmpty()) {
                if (getDbServerByAlias(request.getAlias()).isPresent()) {
                    throw new IllegalArgumentException("数据库服务器别名 " + request.getAlias() + " 已存在");
                }
            }

            String sql = "INSERT INTO `" + DB_SERVER_TABLE + "` " +
                    "(`name`, `host`, `source_type`, `jdbc_url`, `username`, `password`, " +
                    "`alias`, `description`, `create_user`) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

            try (Connection connection = getConnection();
                 PreparedStatement pstmt = connection.prepareStatement(sql)) {
                pstmt.setString(1, request.getName());
                pstmt.setString(2, request.getHost());
                pstmt.setString(3, request.getSourceType());
                pstmt.setString(4, request.getJdbcUrl());
                pstmt.setString(5, request.getUsername());
                pstmt.setString(6, request.getPassword());
                pstmt.setString(7, request.getAlias());
                pstmt.setString(8, request.getDescription());
                pstmt.setString(9, request.getCreateUser());

                int result = pstmt.executeUpdate();
                if (result > 0) {
                    logger.info("成功创建数据库服务器: {}", request.getName());
                    return true;
                } else {
                    logger.error("创建数据库服务器失败: {}", request.getName());
                    return false;
                }
            }

        } catch (SQLException e) {
            logger.error("创建数据库服务器异常: {}", request.getName(), e);
            throw new RuntimeException("创建数据库服务器失败: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean updateDbServer(Integer id, UpdateDbServerRequest request) {
        try {
            logger.info("更新数据库服务器信息: {}", id);

            // 检查服务器是否存在
            if (!existsDbServerId(id)) {
                throw new IllegalArgumentException("数据库服务器ID " + id + " 不存在");
            }

            // 检查名称是否与其他服务器冲突
            if (request.getName() != null && !request.getName().trim().isEmpty()) {
                Optional<DbServerDTO> existingByName = getDbServerByName(request.getName());
                if (existingByName.isPresent() && !existingByName.get().getId().equals(id)) {
                    throw new IllegalArgumentException("数据库服务器名称 " + request.getName() + " 已存在");
                }
            }

            // 检查别名是否与其他服务器冲突
            if (request.getAlias() != null && !request.getAlias().trim().isEmpty()) {
                Optional<DbServerDTO> existingByAlias = getDbServerByAlias(request.getAlias());
                if (existingByAlias.isPresent() && !existingByAlias.get().getId().equals(id)) {
                    throw new IllegalArgumentException("数据库服务器别名 " + request.getAlias() + " 已存在");
                }
            }

            // 构建动态更新SQL
            List<String> updateFields = new ArrayList<>();
            List<Object> params = new ArrayList<>();

            if (request.getName() != null) {
                updateFields.add("`name` = ?");
                params.add(request.getName());
            }
            if (request.getHost() != null) {
                updateFields.add("`host` = ?");
                params.add(request.getHost());
            }
            if (request.getSourceType() != null) {
                updateFields.add("`source_type` = ?");
                params.add(request.getSourceType());
            }
            if (request.getJdbcUrl() != null) {
                updateFields.add("`jdbc_url` = ?");
                params.add(request.getJdbcUrl());
            }
            if (request.getUsername() != null) {
                updateFields.add("`username` = ?");
                params.add(request.getUsername());
            }
            if (request.getPassword() != null) {
                updateFields.add("`password` = ?");
                params.add(request.getPassword());
            }
            if (request.getAlias() != null) {
                updateFields.add("`alias` = ?");
                params.add(request.getAlias());
            }
            if (request.getDescription() != null) {
                updateFields.add("`description` = ?");
                params.add(request.getDescription());
            }
            if (request.getUpdateUser() != null) {
                updateFields.add("`update_user` = ?");
                params.add(request.getUpdateUser());
            }

            if (updateFields.isEmpty()) {
                logger.warn("更新数据库服务器信息时没有提供任何字段: {}", id);
                return true;
            }

            updateFields.add("`update_time` = CURRENT_TIMESTAMP");
            params.add(id);

            String sql = "UPDATE `" + DB_SERVER_TABLE + "` SET " + String.join(", ", updateFields) +
                    " WHERE `id` = ?";

            try (Connection connection = getConnection();
                 PreparedStatement pstmt = connection.prepareStatement(sql)) {
                for (int i = 0; i < params.size(); i++) {
                    pstmt.setObject(i + 1, params.get(i));
                }

                int result = pstmt.executeUpdate();
                if (result > 0) {
                    logger.info("成功更新数据库服务器: {}", id);
                    return true;
                } else {
                    logger.error("更新数据库服务器失败: {}", id);
                    return false;
                }
            }

        } catch (SQLException e) {
            logger.error("更新数据库服务器异常: {}", id, e);
            throw new RuntimeException("更新数据库服务器失败: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean deleteDbServer(Integer id) {
        try {
            logger.info("删除数据库服务器: {}", id);

            String sql = "DELETE FROM `" + DB_SERVER_TABLE + "` WHERE `id` = ?";

            try (Connection connection = getConnection();
                 PreparedStatement pstmt = connection.prepareStatement(sql)) {
                pstmt.setInt(1, id);
                int result = pstmt.executeUpdate();

                if (result > 0) {
                    logger.info("成功删除数据库服务器: {}", id);
                    return true;
                } else {
                    logger.error("删除数据库服务器失败，服务器不存在: {}", id);
                    return false;
                }
            }

        } catch (SQLException e) {
            logger.error("删除数据库服务器异常: {}", id, e);
            throw new RuntimeException("删除数据库服务器失败: " + e.getMessage(), e);
        }
    }

    @Override
    public PageResult<DbServerDTO> searchDbServers(DbServerSearchCriteria criteria, int page, int size) {
        try {
            logger.info("搜索数据库服务器，条件: {}", criteria);

            // 构建查询条件
            List<String> conditions = new ArrayList<>();
            List<Object> params = new ArrayList<>();

            if (criteria.getKeyword() != null && !criteria.getKeyword().trim().isEmpty()) {
                String keyword = "%" + criteria.getKeyword().trim() + "%";
                conditions.add("(`name` LIKE ? OR `alias` LIKE ? OR `description` LIKE ?)");
                params.add(keyword);
                params.add(keyword);
                params.add(keyword);
            }

            if (criteria.getSourceType() != null && !criteria.getSourceType().trim().isEmpty()) {
                conditions.add("`source_type` = ?");
                params.add(criteria.getSourceType());
            }

            if (criteria.getHost() != null && !criteria.getHost().trim().isEmpty()) {
                conditions.add("`host` = ?");
                params.add(criteria.getHost());
            }

            if (criteria.getCreateUser() != null && !criteria.getCreateUser().trim().isEmpty()) {
                conditions.add("`create_user` = ?");
                params.add(criteria.getCreateUser());
            }

            if (criteria.getCreateTimeStart() != null) {
                conditions.add("`create_time` >= ?");
                params.add(Timestamp.valueOf(criteria.getCreateTimeStart()));
            }

            if (criteria.getCreateTimeEnd() != null) {
                conditions.add("`create_time` <= ?");
                params.add(Timestamp.valueOf(criteria.getCreateTimeEnd()));
            }

            String whereClause = conditions.isEmpty() ? "" : " WHERE " + String.join(" AND ", conditions);

            // 查询总数
            String countSql = "SELECT COUNT(*) FROM `" + DB_SERVER_TABLE + "`" + whereClause;
            long total = 0;
            try (Connection connection = getConnection();
                 PreparedStatement pstmt = connection.prepareStatement(countSql)) {
                for (int i = 0; i < params.size(); i++) {
                    pstmt.setObject(i + 1, params.get(i));
                }
                ResultSet rs = pstmt.executeQuery();
                if (rs.next()) {
                    total = rs.getLong(1);
                }
            }

            // 分页查询数据
            String sql = "SELECT `id`, `name`, `host`, `source_type`, `jdbc_url`, `username`, `password`, " +
                    "`alias`, `description`, `create_user`, `create_time`, `update_user`, `update_time` " +
                    "FROM `" + DB_SERVER_TABLE + "`" + whereClause + " " +
                    "ORDER BY `create_time` DESC " +
                    "LIMIT ? OFFSET ?";

            List<DbServerDTO> servers = new ArrayList<>();
            try (Connection connection = getConnection();
                 PreparedStatement pstmt = connection.prepareStatement(sql)) {
                for (int i = 0; i < params.size(); i++) {
                    pstmt.setObject(i + 1, params.get(i));
                }
                pstmt.setInt(params.size() + 1, size);
                pstmt.setInt(params.size() + 2, page * size);

                ResultSet rs = pstmt.executeQuery();
                while (rs.next()) {
                    servers.add(mapResultSetToDTO(rs));
                }
            }

            logger.info("搜索完成，找到 {} 条记录", total);
            return new PageResult<>(servers, total, page, size);

        } catch (SQLException e) {
            logger.error("搜索数据库服务器失败", e);
            throw new RuntimeException("搜索数据库服务器失败: " + e.getMessage(), e);
        }
    }

    @Override
    public BatchOperationResult batchDeleteDbServers(List<Integer> ids) {
        List<String> errors = new ArrayList<>();
        int success = 0;
        int failed = 0;

        for (Integer id : ids) {
            try {
                if (deleteDbServer(id)) {
                    success++;
                } else {
                    failed++;
                    errors.add("删除失败: " + id);
                }
            } catch (Exception e) {
                failed++;
                errors.add("删除异常: " + id + " - " + e.getMessage());
            }
        }

        return new BatchOperationResult(success, failed, ids.size(), errors);
    }

    @Override
    public boolean existsDbServerId(Integer id) {
        try {
            String sql = "SELECT 1 FROM `" + DB_SERVER_TABLE + "` WHERE `id` = ?";
            try (Connection connection = getConnection();
                 PreparedStatement pstmt = connection.prepareStatement(sql)) {
                pstmt.setInt(1, id);
                ResultSet rs = pstmt.executeQuery();
                return rs.next();
            }
        } catch (SQLException e) {
            logger.error("检查数据库服务器ID是否存在失败: {}", id, e);
            return false;
        }
    }

    @Override
    public Optional<DbServerDTO> getDbServerByName(String name) {
        try {
            String sql = "SELECT `id`, `name`, `host`, `source_type`, `jdbc_url`, `username`, `password`, " +
                    "`alias`, `description`, `create_user`, `create_time`, `update_user`, `update_time` " +
                    "FROM `" + DB_SERVER_TABLE + "` WHERE `name` = ?";

            try (Connection connection = getConnection();
                 PreparedStatement pstmt = connection.prepareStatement(sql)) {
                pstmt.setString(1, name);
                ResultSet rs = pstmt.executeQuery();

                if (rs.next()) {
                    return Optional.of(mapResultSetToDTO(rs));
                }
            }

            return Optional.empty();

        } catch (SQLException e) {
            logger.error("根据名称获取数据库服务器失败: {}", name, e);
            throw new RuntimeException("根据名称获取数据库服务器失败: " + e.getMessage(), e);
        }
    }

    @Override
    public Optional<DbServerDTO> getDbServerByAlias(String alias) {
        try {
            String sql = "SELECT `id`, `name`, `host`, `source_type`, `jdbc_url`, `username`, `password`, " +
                    "`alias`, `description`, `create_user`, `create_time`, `update_user`, `update_time` " +
                    "FROM `" + DB_SERVER_TABLE + "` WHERE `alias` = ?";

            try (Connection connection = getConnection();
                 PreparedStatement pstmt = connection.prepareStatement(sql)) {
                pstmt.setString(1, alias);
                ResultSet rs = pstmt.executeQuery();

                if (rs.next()) {
                    return Optional.of(mapResultSetToDTO(rs));
                }
            }

            return Optional.empty();

        } catch (SQLException e) {
            logger.error("根据别名获取数据库服务器失败: {}", alias, e);
            throw new RuntimeException("根据别名获取数据库服务器失败: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean testDbConnection(Integer id) {
        try {
            Optional<DbServerDTO> serverOpt = getDbServerById(id);
            if (!serverOpt.isPresent()) {
                logger.error("测试连接失败，数据库服务器不存在: {}", id);
                return false;
            }

            DbServerDTO server = serverOpt.get();
            logger.info("测试数据库连接: {}", server.getName());

            // 根据数据库类型选择对应的驱动
            String driverClass = getDriverClass(server.getSourceType());
            if (driverClass == null) {
                logger.error("不支持的数据库类型: {}", server.getSourceType());
                return false;
            }

            Class.forName(driverClass);
            try (Connection connection = DriverManager.getConnection(
                    server.getJdbcUrl(), server.getUsername(), server.getPassword())) {
                boolean isValid = connection.isValid(5); // 5秒超时
                logger.info("数据库连接测试{}：{}", isValid ? "成功" : "失败", server.getName());
                return isValid;
            }

        } catch (Exception e) {
            logger.error("测试数据库连接失败: {}", id, e);
            return false;
        }
    }

    @Override
    public List<DbServerDTO> getDbServersBySourceType(String sourceType) {
        try {
            String sql = "SELECT `id`, `name`, `host`, `source_type`, `jdbc_url`, `username`, `password`, " +
                    "`alias`, `description`, `create_user`, `create_time`, `update_user`, `update_time` " +
                    "FROM `" + DB_SERVER_TABLE + "` WHERE `source_type` = ? ORDER BY `create_time` DESC";

            List<DbServerDTO> servers = new ArrayList<>();
            try (Connection connection = getConnection();
                 PreparedStatement pstmt = connection.prepareStatement(sql)) {
                pstmt.setString(1, sourceType);
                ResultSet rs = pstmt.executeQuery();

                while (rs.next()) {
                    servers.add(mapResultSetToDTO(rs));
                }
            }

            return servers;

        } catch (SQLException e) {
            logger.error("根据数据库类型获取服务器列表失败: {}", sourceType, e);
            throw new RuntimeException("根据数据库类型获取服务器列表失败: " + e.getMessage(), e);
        }
    }

    @Override
    public List<String> getSupportedSourceTypes() {
        return new ArrayList<>(SUPPORTED_SOURCE_TYPES);
    }

    /**
     * 将ResultSet映射为DbServerDTO
     */
    private DbServerDTO mapResultSetToDTO(ResultSet rs) throws SQLException {
        DbServerDTO dto = new DbServerDTO();
        dto.setId(rs.getInt("id"));
        dto.setName(rs.getString("name"));
        dto.setHost(rs.getString("host"));
        dto.setSourceType(rs.getString("source_type"));
        dto.setJdbcUrl(rs.getString("jdbc_url"));
        dto.setUsername(rs.getString("username"));
        dto.setPassword(rs.getString("password"));
        dto.setAlias(rs.getString("alias"));
        dto.setDescription(rs.getString("description"));
        dto.setCreateUser(rs.getString("create_user"));
        dto.setUpdateUser(rs.getString("update_user"));

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

    /**
     * 根据数据库类型获取对应的驱动类名
     */
    private String getDriverClass(String sourceType) {
        switch (sourceType.toLowerCase()) {
            case "mysql":
                return "com.mysql.cj.jdbc.Driver";
            case "oracle":
                return "oracle.jdbc.driver.OracleDriver";
            case "postgresql":
                return "org.postgresql.Driver";
            case "sqlserver":
                return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
            case "tdsql":
                return "com.mysql.cj.jdbc.Driver"; // TDSQL兼容MySQL协议
            case "clickhouse":
                return "com.clickhouse.jdbc.ClickHouseDriver";
            case "tidb":
                return "com.mysql.cj.jdbc.Driver"; // TiDB兼容MySQL协议
            default:
                return null;
        }
    }

    @Override
    public List<String> getDatabases(Integer id) throws SQLException {
        // 获取数据源信息
        Optional<DbServerDTO> serverOpt = getDbServerById(id);
        if (!serverOpt.isPresent()) {
            logger.error("获取数据库列表失败，数据库服务器不存在: {}", id);
            throw new RuntimeException("获取数据库列表失败，数据库服务器不存在: " + id);
        }

        DbServerDTO server = serverOpt.get();
        // 创建JDBC数据库连接
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(server.getJdbcUrl(), server.getUsername(), server.getPassword());
        } catch (SQLException e) {
            logger.error("创建JDBC数据库连接失败: {}", id, e);
            throw new RuntimeException("创建JDBC数据库连接失败: " + e.getMessage(), e);
        }

        List<String> databases = new ArrayList<>();
        // 获取数据库服务器中的数据库列表
        try {
            if (server.getSourceType().equalsIgnoreCase("mysql") || 
                server.getSourceType().equalsIgnoreCase("tdsql") || 
                server.getSourceType().equalsIgnoreCase("tidb")) {
            String sql = "SHOW DATABASES";
            try (Statement stmt = connection.createStatement()) {
                ResultSet rs = stmt.executeQuery(sql);
                while (rs.next()) {
                    databases.add(rs.getString(1));
                }
            }
        } else if (server.getSourceType().equalsIgnoreCase("oracle")) {
            // 获取Oracle数据库中的数据库列表
            // Oracle获取所有schema（用户）
            String sql = "SELECT DISTINCT OWNER FROM ALL_TABLES WHERE OWNER NOT IN ('SYS', 'SYSTEM', 'OUTLN', 'DIP', 'ORACLE_OCM', 'DBSNMP', 'APPQOSSYS', 'WMSYS', 'EXFSYS', 'CTXSYS', 'ANONYMOUS', 'XDB', 'ORDPLUGINS', 'ORDSYS', 'SI_INFORMTN_SCHEMA', 'MDSYS', 'OLAPSYS', 'MDDATA', 'XS$NULL', 'OJVMSYS', 'GSMADMIN_INTERNAL', 'ORDDATA', 'FLOWS_FILES', 'APEX_040000', 'APEX_PUBLIC_USER') ORDER BY OWNER";
            try (Statement stmt = connection.createStatement()) {
                ResultSet rs = stmt.executeQuery(sql);
                while (rs.next()) {
                    databases.add(rs.getString(1));
                }
            }
        } else if (server.getSourceType().equalsIgnoreCase("postgresql")) {
            // 获取PostgreSQL数据库中的数据库列表
            String sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ?";
            try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
                pstmt.setString(1, "public");
                ResultSet rs = pstmt.executeQuery();
                while (rs.next()) {
                    databases.add(rs.getString(1));
                }
            }   
        } else if (server.getSourceType().equalsIgnoreCase("sqlserver")) {
            // 获取SQL Server数据库中的数据库列表
            String sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ?";
            try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
                pstmt.setString(1, "dbo");
                ResultSet rs = pstmt.executeQuery();
                while (rs.next()) {
                    databases.add(rs.getString(1));
                }
            }
        } else if (server.getSourceType().equalsIgnoreCase("clickhouse")) {
            // 获取ClickHouse数据库中的数据库列表
            String sql = "SHOW TABLES";
            try (Statement stmt = connection.createStatement()) {
                ResultSet rs = stmt.executeQuery(sql);
                while (rs.next()) {
                    databases.add(rs.getString(1));
                }
            }
        } else {
            logger.error("不支持的数据库类型: {}", server.getSourceType());
            throw new RuntimeException("不支持的数据库类型: " + server.getSourceType());
        }
        } catch (SQLException e) {
            logger.error("获取数据库列表失败: {}", id, e);
            throw new RuntimeException("获取数据库列表失败: " + e.getMessage(), e);
        }

        // 关闭数据库连接
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            logger.error("关闭数据库连接失败: {}", id, e);
        }

        return databases;
    }

    @Override
    public List<BusinessTableInfoDTO> getBusinessTables(Integer id, String database) throws SQLException {
        logger.info("开始获取业务表列表: serverId={}, database={}", id, database);
        
        // 获取数据源信息
        Optional<DbServerDTO> serverOpt = getDbServerById(id);
        if (!serverOpt.isPresent()) {
            logger.error("获取业务表列表失败，数据库服务器不存在: {}", id);
            throw new RuntimeException("获取业务表列表失败，数据库服务器不存在: " + id);
        }
        DbServerDTO server = serverOpt.get();   
        logger.info("找到数据源: {}, 类型: {}", server.getName(), server.getSourceType());
        
        // 创建JDBC数据库连接
        Connection connection = null;
        try {
            logger.info("创建数据库连接: {}", server.getJdbcUrl());
            connection = DriverManager.getConnection(server.getJdbcUrl(), server.getUsername(), server.getPassword());
            logger.info("数据库连接创建成功");
        } catch (SQLException e) {
            logger.error("创建JDBC数据库连接失败: serverId={}, jdbcUrl={}", id, server.getJdbcUrl(), e);
            throw new RuntimeException("创建JDBC数据库连接失败: " + e.getMessage(), e);
        }

        // 根据数据源的类型获取业务表详细信息列表
        List<BusinessTableInfoDTO> businessTables = new ArrayList<>();
        try {
            if (server.getSourceType().equalsIgnoreCase("mysql") || 
                server.getSourceType().equalsIgnoreCase("tdsql") || 
                server.getSourceType().equalsIgnoreCase("tidb")) {
                // 获取MySQL/TDSQL/TiDB数据库中的业务表详细信息
                String sql = "SELECT TABLE_NAME, TABLE_COMMENT, ENGINE, CREATE_TIME, UPDATE_TIME, " +
                           "TABLE_TYPE, TABLE_ROWS, DATA_LENGTH " +
                           "FROM INFORMATION_SCHEMA.TABLES " +
                           "WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'";
                logger.info("执行MySQL查询SQL: {}, 参数: {}", sql, database);
                try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
                    pstmt.setString(1, database);
                    ResultSet rs = pstmt.executeQuery();
                    int count = 0;
                    while (rs.next()) {
                        BusinessTableInfoDTO dto = new BusinessTableInfoDTO();
                        dto.setTableName(rs.getString("TABLE_NAME"));
                        dto.setTableComment(rs.getString("TABLE_COMMENT"));
                        dto.setDatabase(database);
                        dto.setEngine(rs.getString("ENGINE"));
                        dto.setTableType(rs.getString("TABLE_TYPE"));
                        dto.setRowCount(rs.getLong("TABLE_ROWS"));
                        dto.setTableSize(rs.getLong("DATA_LENGTH"));
                        
                        // 处理时间字段
                        Timestamp createTime = rs.getTimestamp("CREATE_TIME");
                        if (createTime != null) {
                            dto.setCreateTime(createTime.toLocalDateTime());
                        }
                        
                        Timestamp updateTime = rs.getTimestamp("UPDATE_TIME");
                        if (updateTime != null) {
                            dto.setUpdateTime(updateTime.toLocalDateTime());
                        }
                        
                        businessTables.add(dto);
                        count++;
                        logger.debug("找到业务表: {}.{}", database, dto.getTableName());
                    }
                    logger.info("MySQL查询完成，找到 {} 个业务表", count);
                }
            } else if (server.getSourceType().equalsIgnoreCase("oracle")) {
                // 获取Oracle数据库中的业务表详细信息
                String sql = "SELECT t.TABLE_NAME, c.COMMENTS AS TABLE_COMMENT, t.NUM_ROWS, " +
                           "(t.BLOCKS * 8192) AS TABLE_SIZE, t.LAST_ANALYZED " +
                           "FROM ALL_TABLES t " +
                           "LEFT JOIN ALL_TAB_COMMENTS c ON t.TABLE_NAME = c.TABLE_NAME AND t.OWNER = c.OWNER " +
                           "WHERE t.OWNER = ? AND t.TABLE_NAME NOT LIKE 'BIN$%'";
                logger.info("sql: {}", sql);
                try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
                    pstmt.setString(1, database);
                    ResultSet rs = pstmt.executeQuery();    
                    while (rs.next()) {
                        BusinessTableInfoDTO dto = new BusinessTableInfoDTO();
                        dto.setTableName(rs.getString("TABLE_NAME"));
                        dto.setTableComment(rs.getString("TABLE_COMMENT"));
                        dto.setDatabase(database);
                        dto.setTableType("BASE TABLE");
                        dto.setRowCount(rs.getLong("NUM_ROWS"));
                        dto.setTableSize(rs.getLong("TABLE_SIZE"));
                        dto.setEngine("ORACLE");

                        if(rs.getTimestamp("LAST_ANALYZED") != null) {
                            dto.setCreateTime(rs.getTimestamp("LAST_ANALYZED").toLocalDateTime());
                            dto.setUpdateTime(rs.getTimestamp("LAST_ANALYZED").toLocalDateTime());
                        }
                        
                        businessTables.add(dto);
                    }
                }
            } else if (server.getSourceType().equalsIgnoreCase("postgresql")) {
                // 获取PostgreSQL数据库中的业务表详细信息
                String sql = "SELECT t.TABLE_NAME, obj_description(c.oid) AS TABLE_COMMENT, " +
                           "t.TABLE_TYPE, s.n_tup_ins + s.n_tup_upd + s.n_tup_del AS ROW_COUNT, " +
                           "pg_total_relation_size(c.oid) AS TABLE_SIZE " +
                           "FROM INFORMATION_SCHEMA.TABLES t " +
                           "LEFT JOIN pg_class c ON c.relname = t.TABLE_NAME " +
                           "LEFT JOIN pg_stat_user_tables s ON s.relname = t.TABLE_NAME " +
                           "WHERE t.TABLE_SCHEMA = ? AND t.TABLE_TYPE = 'BASE TABLE'";
                try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
                    pstmt.setString(1, database);
                    ResultSet rs = pstmt.executeQuery();
                    while (rs.next()) {
                        BusinessTableInfoDTO dto = new BusinessTableInfoDTO();
                        dto.setTableName(rs.getString("TABLE_NAME"));
                        dto.setTableComment(rs.getString("TABLE_COMMENT"));
                        dto.setDatabase(database);
                        dto.setTableType(rs.getString("TABLE_TYPE"));
                        dto.setRowCount(rs.getLong("ROW_COUNT"));
                        dto.setTableSize(rs.getLong("TABLE_SIZE"));
                        
                        businessTables.add(dto);
                    }
                }
            } else if (server.getSourceType().equalsIgnoreCase("sqlserver")) {
                // 获取SQL Server数据库中的业务表详细信息
                String sql = "SELECT t.TABLE_NAME, ep.value AS TABLE_COMMENT, t.TABLE_TYPE, " +
                           "p.rows AS ROW_COUNT, " +
                           "SUM(a.total_pages) * 8 * 1024 AS TABLE_SIZE " +
                           "FROM INFORMATION_SCHEMA.TABLES t " +
                           "LEFT JOIN sys.tables st ON st.name = t.TABLE_NAME " +
                           "LEFT JOIN sys.extended_properties ep ON ep.major_id = st.object_id AND ep.minor_id = 0 " +
                           "LEFT JOIN sys.partitions p ON p.object_id = st.object_id " +
                           "LEFT JOIN sys.allocation_units a ON a.container_id = p.partition_id " +
                           "WHERE t.TABLE_SCHEMA = ? AND t.TABLE_TYPE = 'BASE TABLE' " +
                           "GROUP BY t.TABLE_NAME, ep.value, t.TABLE_TYPE, p.rows";
                try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
                    pstmt.setString(1, database);
                    ResultSet rs = pstmt.executeQuery();
                    while (rs.next()) {
                        BusinessTableInfoDTO dto = new BusinessTableInfoDTO();
                        dto.setTableName(rs.getString("TABLE_NAME"));
                        dto.setTableComment(rs.getString("TABLE_COMMENT"));
                        dto.setDatabase(database);
                        dto.setTableType(rs.getString("TABLE_TYPE"));
                        dto.setRowCount(rs.getLong("ROW_COUNT"));
                        dto.setTableSize(rs.getLong("TABLE_SIZE"));
                        
                        businessTables.add(dto);
                    }
                }
            } else if (server.getSourceType().equalsIgnoreCase("clickhouse")) {
                // 获取ClickHouse数据库中的业务表详细信息
                String sql = "SELECT name AS TABLE_NAME, comment AS TABLE_COMMENT, engine AS ENGINE, " +
                           "total_rows AS ROW_COUNT, total_bytes AS TABLE_SIZE " +
                           "FROM system.tables " +
                           "WHERE database = ? AND engine NOT LIKE '%View'";
                try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
                    pstmt.setString(1, database);
                    ResultSet rs = pstmt.executeQuery();
                    while (rs.next()) {
                        BusinessTableInfoDTO dto = new BusinessTableInfoDTO();
                        dto.setTableName(rs.getString("TABLE_NAME"));
                        dto.setTableComment(rs.getString("TABLE_COMMENT"));
                        dto.setDatabase(database);
                        dto.setEngine(rs.getString("ENGINE"));
                        dto.setTableType("BASE TABLE");
                        dto.setRowCount(rs.getLong("ROW_COUNT"));
                        dto.setTableSize(rs.getLong("TABLE_SIZE"));
                        
                        businessTables.add(dto);
                    }
                }
            } else {
                logger.error("不支持的数据库类型: {}", server.getSourceType());
                throw new RuntimeException("不支持的数据库类型: " + server.getSourceType());
            }
        } finally {
            // 关闭数据库连接
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                logger.error("关闭数据库连接失败: {}", id, e);
            }
        }

        logger.info("获取业务表列表完成: serverId={}, database={}, 总共找到 {} 个业务表", id, database, businessTables.size());
        return businessTables;
    }

    @Override
    public TableSchemaInfoDTO getTableSchema(Integer id, String database, String tableName) throws SQLException {
        logger.info("开始获取表结构: serverId={}, database={}, tableName={}", id, database, tableName);
        
        // 获取数据源信息
        Optional<DbServerDTO> serverOpt = getDbServerById(id);
        if (!serverOpt.isPresent()) {
            logger.error("获取表结构失败，数据库服务器不存在: {}", id);
            throw new RuntimeException("获取表结构失败，数据库服务器不存在: " + id);
        }
        DbServerDTO server = serverOpt.get();
        logger.info("找到数据源: {}, 类型: {}", server.getName(), server.getSourceType());
        
        // 创建JDBC数据库连接
        Connection connection = null;
        TableSchemaInfoDTO schemaInfo = new TableSchemaInfoDTO();
        schemaInfo.setTableName(tableName);
        schemaInfo.setDatabase(database);
        
        try {
            logger.info("创建数据库连接: {}", server.getJdbcUrl());
            connection = DriverManager.getConnection(server.getJdbcUrl(), server.getUsername(), server.getPassword());
            logger.info("数据库连接创建成功");
            
            List<TableFieldInfoDTO> fields = new ArrayList<>();
            List<String> primaryKeys = new ArrayList<>();
            
            // 获取表结构
            if(server.getSourceType().equalsIgnoreCase("mysql") || 
               server.getSourceType().equalsIgnoreCase("tdsql") || 
               server.getSourceType().equalsIgnoreCase("tidb")) {
                // 获取MySQL/TDSQL/TiDB数据库中的表结构
                String columnsSql = "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_COMMENT, COLUMN_KEY, IS_NULLABLE, COLUMN_DEFAULT " +
                                   "FROM INFORMATION_SCHEMA.COLUMNS " +
                                   "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? " +
                                   "ORDER BY ORDINAL_POSITION";
                try (PreparedStatement pstmt = connection.prepareStatement(columnsSql)) {
                    pstmt.setString(1, database);
                    pstmt.setString(2, tableName);
                    ResultSet rs = pstmt.executeQuery();
                    while (rs.next()) {
                        String columnName = rs.getString("COLUMN_NAME");
                        String dataType = rs.getString("DATA_TYPE");
                        String comment = rs.getString("COLUMN_COMMENT");
                        String columnKey = rs.getString("COLUMN_KEY");
                        boolean isNullable = "YES".equalsIgnoreCase(rs.getString("IS_NULLABLE"));
                        String defaultValue = rs.getString("COLUMN_DEFAULT");
                        boolean isPrimaryKey = "PRI".equalsIgnoreCase(columnKey);
                        
                        if (isPrimaryKey) {
                            primaryKeys.add(columnName);
                        }
                        
                        TableFieldInfoDTO field = new TableFieldInfoDTO();
                        field.setFieldName(columnName);
                        field.setFieldType(dataType);
                        field.setComment(comment);
                        field.setNullable(isNullable);
                        field.setDefaultValue(defaultValue);
                        field.setPrimaryKey(isPrimaryKey);
                        fields.add(field);
                        
                        logger.debug("找到字段: {}, 类型: {}, 主键: {}", columnName, dataType, isPrimaryKey);
                    }
                }
                
                // 获取表注释
                String tableCommentSql = "SELECT TABLE_COMMENT FROM INFORMATION_SCHEMA.TABLES " +
                                        "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";
                try (PreparedStatement pstmt = connection.prepareStatement(tableCommentSql)) {
                    pstmt.setString(1, database);
                    pstmt.setString(2, tableName);
                    ResultSet rs = pstmt.executeQuery();
                    if (rs.next()) {
                        schemaInfo.setComment(rs.getString("TABLE_COMMENT"));
                    }
                }
                
            } else if (server.getSourceType().equalsIgnoreCase("oracle")) {
                // 获取Oracle数据库中的表结构
                String columnsSql = "SELECT COLUMN_NAME, DATA_TYPE, NULLABLE, DATA_DEFAULT " +
                                   "FROM ALL_TAB_COLUMNS " +
                                   "WHERE OWNER = ? AND TABLE_NAME = ? " +
                                   "ORDER BY COLUMN_ID";
                try (PreparedStatement pstmt = connection.prepareStatement(columnsSql)) {
                    pstmt.setString(1, database);
                    pstmt.setString(2, tableName);
                    ResultSet rs = pstmt.executeQuery();
                    while (rs.next()) {
                        String columnName = rs.getString("COLUMN_NAME");
                        String dataType = rs.getString("DATA_TYPE");
                        boolean isNullable = "Y".equalsIgnoreCase(rs.getString("NULLABLE"));
                        String defaultValue = rs.getString("DATA_DEFAULT");
                        
                        TableFieldInfoDTO field = new TableFieldInfoDTO();
                        field.setFieldName(columnName);
                        field.setFieldType(dataType);
                        field.setNullable(isNullable);
                        field.setDefaultValue(defaultValue);
                        field.setPrimaryKey(false);
                        fields.add(field);
                    }
                }
                
                // 获取主键信息
                String primaryKeysSql = "SELECT COLUMN_NAME FROM ALL_CONS_COLUMNS " +
                                       "WHERE OWNER = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME IN " +
                                       "(SELECT CONSTRAINT_NAME FROM ALL_CONSTRAINTS " +
                                       "WHERE OWNER = ? AND TABLE_NAME = ? AND CONSTRAINT_TYPE = 'P')";
                try (PreparedStatement pstmt = connection.prepareStatement(primaryKeysSql)) {
                    pstmt.setString(1, database);
                    pstmt.setString(2, tableName);
                    pstmt.setString(3, database);
                    pstmt.setString(4, tableName);
                    ResultSet rs = pstmt.executeQuery();
                    while (rs.next()) {
                        String columnName = rs.getString("COLUMN_NAME");
                        primaryKeys.add(columnName);
                        // 更新对应字段的主键标识
                        fields.stream()
                              .filter(field -> field.getFieldName().equals(columnName))
                              .forEach(field -> field.setPrimaryKey(true));
                    }
                }
                
                // 获取字段注释
                String commentsSql = "SELECT COLUMN_NAME, COMMENTS FROM ALL_COL_COMMENTS " +
                                    "WHERE OWNER = ? AND TABLE_NAME = ?";
                try (PreparedStatement pstmt = connection.prepareStatement(commentsSql)) {
                    pstmt.setString(1, database);
                    pstmt.setString(2, tableName);
                    ResultSet rs = pstmt.executeQuery();
                    while (rs.next()) {
                        String columnName = rs.getString("COLUMN_NAME");
                        String comment = rs.getString("COMMENTS");
                        fields.stream()
                              .filter(field -> field.getFieldName().equals(columnName))
                              .forEach(field -> field.setComment(comment));
                    }
                }
                
                // 获取表注释
                String tableCommentSql = "SELECT COMMENTS FROM ALL_TAB_COMMENTS " +
                                        "WHERE OWNER = ? AND TABLE_NAME = ?";
                try (PreparedStatement pstmt = connection.prepareStatement(tableCommentSql)) {
                    pstmt.setString(1, database);
                    pstmt.setString(2, tableName);
                    ResultSet rs = pstmt.executeQuery();
                    if (rs.next()) {
                        schemaInfo.setComment(rs.getString("COMMENTS"));
                    }
                }
                
            } else if (server.getSourceType().equalsIgnoreCase("postgresql")) {
                // 获取PostgreSQL数据库中的表结构
                String columnsSql = "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT " +
                                   "FROM INFORMATION_SCHEMA.COLUMNS " +
                                   "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? " +
                                   "ORDER BY ORDINAL_POSITION";
                try (PreparedStatement pstmt = connection.prepareStatement(columnsSql)) {
                    pstmt.setString(1, database);
                    pstmt.setString(2, tableName);
                    ResultSet rs = pstmt.executeQuery();
                    while (rs.next()) {
                        String columnName = rs.getString("COLUMN_NAME");
                        String dataType = rs.getString("DATA_TYPE");
                        boolean isNullable = "YES".equalsIgnoreCase(rs.getString("IS_NULLABLE"));
                        String defaultValue = rs.getString("COLUMN_DEFAULT");
                        
                        TableFieldInfoDTO field = new TableFieldInfoDTO();
                        field.setFieldName(columnName);
                        field.setFieldType(dataType);
                        field.setNullable(isNullable);
                        field.setDefaultValue(defaultValue);
                        field.setPrimaryKey(false);
                        fields.add(field);
                    }
                }
                
                // 获取主键信息
                String primaryKeysSql = "SELECT a.attname AS column_name " +
                                       "FROM pg_index i " +
                                       "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) " +
                                       "WHERE i.indrelid = (SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = ? AND n.nspname = ?) " +
                                       "AND i.indisprimary";
                try (PreparedStatement pstmt = connection.prepareStatement(primaryKeysSql)) {
                    pstmt.setString(1, tableName);
                    pstmt.setString(2, database);
                    ResultSet rs = pstmt.executeQuery();
                    while (rs.next()) {
                        String columnName = rs.getString("column_name");
                        primaryKeys.add(columnName);
                        // 更新对应字段的主键标识
                        fields.stream()
                              .filter(field -> field.getFieldName().equals(columnName))
                              .forEach(field -> field.setPrimaryKey(true));
                    }
                }
                
            } else if (server.getSourceType().equalsIgnoreCase("sqlserver")) {
                // 获取SQL Server数据库中的表结构
                String columnsSql = "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT " +
                                   "FROM INFORMATION_SCHEMA.COLUMNS " +
                                   "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? " +
                                   "ORDER BY ORDINAL_POSITION";
                try (PreparedStatement pstmt = connection.prepareStatement(columnsSql)) {
                    pstmt.setString(1, database);
                    pstmt.setString(2, tableName);
                    ResultSet rs = pstmt.executeQuery();
                    while (rs.next()) {
                        String columnName = rs.getString("COLUMN_NAME");
                        String dataType = rs.getString("DATA_TYPE");
                        boolean isNullable = "YES".equalsIgnoreCase(rs.getString("IS_NULLABLE"));
                        String defaultValue = rs.getString("COLUMN_DEFAULT");
                        
                        TableFieldInfoDTO field = new TableFieldInfoDTO();
                        field.setFieldName(columnName);
                        field.setFieldType(dataType);
                        field.setNullable(isNullable);
                        field.setDefaultValue(defaultValue);
                        field.setPrimaryKey(false);
                        fields.add(field);
                    }
                }
                
                // 获取主键信息
                String primaryKeysSql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE " +
                                       "WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1 " +
                                       "AND TABLE_SCHEMA = ? AND TABLE_NAME = ?";
                try (PreparedStatement pstmt = connection.prepareStatement(primaryKeysSql)) {
                    pstmt.setString(1, database);
                    pstmt.setString(2, tableName);
                    ResultSet rs = pstmt.executeQuery();
                    while (rs.next()) {
                        String columnName = rs.getString("COLUMN_NAME");
                        primaryKeys.add(columnName);
                        // 更新对应字段的主键标识
                        fields.stream()
                              .filter(field -> field.getFieldName().equals(columnName))
                              .forEach(field -> field.setPrimaryKey(true));
                    }
                }
                
            } else if (server.getSourceType().equalsIgnoreCase("clickhouse")) {
                // 获取ClickHouse数据库中的表结构
                String columnsSql = "SELECT name, type, default_expression, is_in_primary_key " +
                                   "FROM system.columns " +
                                   "WHERE database = ? AND table = ? " +
                                   "ORDER BY position";
                try (PreparedStatement pstmt = connection.prepareStatement(columnsSql)) {
                    pstmt.setString(1, database);
                    pstmt.setString(2, tableName);
                    ResultSet rs = pstmt.executeQuery();
                    while (rs.next()) {
                        String columnName = rs.getString("name");
                        String dataType = rs.getString("type");
                        String defaultValue = rs.getString("default_expression");
                        boolean isPrimaryKey = rs.getBoolean("is_in_primary_key");
                        
                        if (isPrimaryKey) {
                            primaryKeys.add(columnName);
                        }
                        
                        TableFieldInfoDTO field = new TableFieldInfoDTO();
                        field.setFieldName(columnName);
                        field.setFieldType(dataType);
                        field.setNullable(dataType.contains("Nullable"));
                        field.setDefaultValue(defaultValue);
                        field.setPrimaryKey(isPrimaryKey);
                        fields.add(field);
                    }
                }
                
            } else {
                logger.error("不支持的数据库类型: {}", server.getSourceType());
                throw new RuntimeException("不支持的数据库类型: " + server.getSourceType());
            }
            
            schemaInfo.setFields(fields);
            schemaInfo.setPrimaryKeys(primaryKeys);
            
            logger.info("获取表结构完成: serverId={}, database={}, tableName={}, 字段数: {}, 主键数: {}", 
                       id, database, tableName, fields.size(), primaryKeys.size());
            
        } catch (SQLException e) {
            logger.error("创建JDBC数据库连接失败: serverId={}, jdbcUrl={}", id, server.getJdbcUrl(), e);
            throw new RuntimeException("创建JDBC数据库连接失败: " + e.getMessage(), e);
        } finally {
            // 关闭数据库连接
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                logger.error("关闭数据库连接失败: {}", id, e);
            }
        }
        
        return schemaInfo;
    }

    @Override
    public List<Map<String, Object>> previewTableData(Integer id, String database, String tableName, int limit) throws SQLException {
        logger.info("开始预览表数据: serverId={}, database={}, tableName={}, limit={}", id, database, tableName, limit);
        
        // 获取数据源信息
        Optional<DbServerDTO> serverOpt = getDbServerById(id);
        if (!serverOpt.isPresent()) {
            logger.error("预览表数据失败，数据库服务器不存在: {}", id);
            throw new RuntimeException("预览表数据失败，数据库服务器不存在: " + id);
        }
        DbServerDTO server = serverOpt.get();
        logger.info("找到数据源: {}, 类型: {}", server.getName(), server.getSourceType());
        
        // 创建JDBC数据库连接
        Connection connection = null;
        List<Map<String, Object>> previewData = new ArrayList<>();
        
        try {
            logger.info("创建数据库连接: {}", server.getJdbcUrl());
            connection = DriverManager.getConnection(server.getJdbcUrl(), server.getUsername(), server.getPassword());
            logger.info("数据库连接创建成功");
            
            // 构建查询SQL
            String sql = "";
            if (server.getSourceType().equalsIgnoreCase("mysql") || 
                server.getSourceType().equalsIgnoreCase("tdsql") || 
                server.getSourceType().equalsIgnoreCase("tidb")) {
                sql = String.format("SELECT * FROM `%s`.`%s` LIMIT %d", database, tableName, limit);
            } else if (server.getSourceType().equalsIgnoreCase("oracle")) {
                sql = String.format("SELECT * FROM %s.%s WHERE ROWNUM <= %d", database, tableName, limit);
            } else if (server.getSourceType().equalsIgnoreCase("postgresql")) {
                sql = String.format("SELECT * FROM %s.%s LIMIT %d", database, tableName, limit);
            } else if (server.getSourceType().equalsIgnoreCase("sqlserver")) {
                sql = String.format("SELECT TOP %d * FROM %s.%s", limit, database, tableName);
            } else if (server.getSourceType().equalsIgnoreCase("clickhouse")) {
                sql = String.format("SELECT * FROM %s.%s LIMIT %d", database, tableName, limit);
            } else {
                logger.error("不支持的数据库类型: {}", server.getSourceType());
                throw new RuntimeException("不支持的数据库类型: " + server.getSourceType());
            }
            
            logger.info("执行预览SQL: {}", sql);
            
            try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
                ResultSet rs = pstmt.executeQuery();
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();
                
                while (rs.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnName(i);
                        Object value = rs.getObject(i);
                        row.put(columnName, value);
                    }
                    previewData.add(row);
                }
                
                logger.info("预览表数据完成: serverId={}, database={}, tableName={}, 返回数据行数: {}", 
                           id, database, tableName, previewData.size());
            }
            
        } catch (SQLException e) {
            logger.error("预览表数据失败: serverId={}, database={}, tableName={}", id, database, tableName, e);
            throw new RuntimeException("预览表数据失败: " + e.getMessage(), e);
        } finally {
            // 关闭数据库连接
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                logger.error("关闭数据库连接失败: {}", id, e);
            }
        }
        
        return previewData;
    }
} 