package cn.com.multi_writer.service.impl;

import cn.com.multi_writer.service.TableApplicationService;
import cn.com.multi_writer.service.dto.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;

/**
 * MySQL实现的表应用关联服务
 */
@Service("mySQLTableApplicationService")
public class MySQLTableApplicationServiceImpl implements TableApplicationService {

    private static final Logger logger = LoggerFactory.getLogger(MySQLTableApplicationServiceImpl.class);

    @Autowired
    private DataSource dataSource;

    // 表名常量
    private static final String TABLE_APPLICATION_TABLE = "hoodie_meta_table_application";
    
    // 支持的表类型
    private static final List<String> SUPPORTED_TABLE_TYPES = Arrays.asList(
        "hudi", "kafka", "es", "jdbc", "mongodb", "redis", "clickhouse"
    );

    private Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    @Override
    public PageResult<TableApplicationDTO> getAllTableApplications(int page, int size) {
        String sql = "SELECT * FROM " + TABLE_APPLICATION_TABLE + " ORDER BY create_time DESC LIMIT ? OFFSET ?";
        String countSql = "SELECT COUNT(*) FROM " + TABLE_APPLICATION_TABLE;
        
        try (Connection conn = getConnection()) {
            // 获取总数
            long total = 0;
            try (PreparedStatement countStmt = conn.prepareStatement(countSql);
                 ResultSet countRs = countStmt.executeQuery()) {
                if (countRs.next()) {
                    total = countRs.getLong(1);
                }
            }

            // 获取数据
            List<TableApplicationDTO> tableApplications = new ArrayList<>();
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setInt(1, size);
                stmt.setInt(2, page * size);
                
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        tableApplications.add(mapResultSetToDTO(rs));
                    }
                }
            }

            return new PageResult<>(tableApplications, total, page, size);

        } catch (SQLException e) {
            logger.error("获取所有表应用关联失败", e);
            return new PageResult<>(new ArrayList<>(), 0, page, size);
        }
    }

    @Override
    public Optional<TableApplicationDTO> getTableApplicationById(Integer id) {
        String sql = "SELECT * FROM " + TABLE_APPLICATION_TABLE + " WHERE id = ?";
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setInt(1, id);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapResultSetToDTO(rs));
                }
            }
            
        } catch (SQLException e) {
            logger.error("根据ID获取表应用关联失败: {}", id, e);
        }
        
        return Optional.empty();
    }

    @Override
    public boolean createTableApplication(CreateTableApplicationRequest request) {
        String sql = "INSERT INTO " + TABLE_APPLICATION_TABLE + " (table_id, application_name, table_type) VALUES (?, ?, ?)";
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, request.getTableId());
            stmt.setString(2, request.getApplicationName());
            stmt.setString(3, request.getTableType());
            
            int rowsAffected = stmt.executeUpdate();
            logger.info("创建表应用关联成功: tableId={}, applicationName={}", 
                       request.getTableId(), request.getApplicationName());
            return rowsAffected > 0;
            
        } catch (SQLException e) {
            logger.error("创建表应用关联失败: tableId={}, applicationName={}", 
                        request.getTableId(), request.getApplicationName(), e);
            return false;
        }
    }

    @Override
    public boolean updateTableApplication(Integer id, UpdateTableApplicationRequest request) {
        List<String> setClauses = new ArrayList<>();
        List<Object> params = new ArrayList<>();
        
        if (request.getTableId() != null) {
            setClauses.add("table_id = ?");
            params.add(request.getTableId());
        }
        if (request.getApplicationName() != null) {
            setClauses.add("application_name = ?");
            params.add(request.getApplicationName());
        }
        if (request.getTableType() != null) {
            setClauses.add("table_type = ?");
            params.add(request.getTableType());
        }
        
        if (setClauses.isEmpty()) {
            return true; // 没有需要更新的字段
        }
        
        setClauses.add("update_time = CURRENT_TIMESTAMP");
        params.add(id);
        
        String sql = "UPDATE " + TABLE_APPLICATION_TABLE + " SET " + String.join(", ", setClauses) + " WHERE id = ?";
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            for (int i = 0; i < params.size(); i++) {
                stmt.setObject(i + 1, params.get(i));
            }
            
            int rowsAffected = stmt.executeUpdate();
            logger.info("更新表应用关联成功: ID={}", id);
            return rowsAffected > 0;
            
        } catch (SQLException e) {
            logger.error("更新表应用关联失败: ID={}", id, e);
            return false;
        }
    }

    @Override
    public boolean deleteTableApplication(Integer id) {
        String sql = "DELETE FROM " + TABLE_APPLICATION_TABLE + " WHERE id = ?";
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setInt(1, id);
            
            int rowsAffected = stmt.executeUpdate();
            logger.info("删除表应用关联成功: ID={}", id);
            return rowsAffected > 0;
            
        } catch (SQLException e) {
            logger.error("删除表应用关联失败: ID={}", id, e);
            return false;
        }
    }

    @Override
    public PageResult<TableApplicationDTO> searchTableApplications(TableApplicationSearchCriteria criteria, int page, int size) {
        List<String> conditions = new ArrayList<>();
        List<Object> params = new ArrayList<>();
        
        if (criteria.getKeyword() != null && !criteria.getKeyword().trim().isEmpty()) {
            conditions.add("(table_id LIKE ? OR application_name LIKE ?)");
            String keyword = "%" + criteria.getKeyword().trim() + "%";
            params.add(keyword);
            params.add(keyword);
        }
        
        if (criteria.getTableId() != null && !criteria.getTableId().trim().isEmpty()) {
            conditions.add("table_id LIKE ?");
            params.add("%" + criteria.getTableId().trim() + "%");
        }
        
        if (criteria.getApplicationName() != null && !criteria.getApplicationName().trim().isEmpty()) {
            conditions.add("application_name LIKE ?");
            params.add("%" + criteria.getApplicationName().trim() + "%");
        }
        
        if (criteria.getTableType() != null && !criteria.getTableType().trim().isEmpty()) {
            conditions.add("table_type = ?");
            params.add(criteria.getTableType().trim());
        }
        
        if (criteria.getCreateTimeStart() != null) {
            conditions.add("create_time >= ?");
            params.add(criteria.getCreateTimeStart());
        }
        
        if (criteria.getCreateTimeEnd() != null) {
            conditions.add("create_time <= ?");
            params.add(criteria.getCreateTimeEnd());
        }
        
        String whereClause = conditions.isEmpty() ? "" : " WHERE " + String.join(" AND ", conditions);
        String sql = "SELECT * FROM " + TABLE_APPLICATION_TABLE + whereClause + " ORDER BY create_time DESC LIMIT ? OFFSET ?";
        String countSql = "SELECT COUNT(*) FROM " + TABLE_APPLICATION_TABLE + whereClause;
        
        params.add(size);
        params.add(page * size);
        
        try (Connection conn = getConnection()) {
            // 获取总数
            long total = 0;
            try (PreparedStatement countStmt = conn.prepareStatement(countSql)) {
                for (int i = 0; i < params.size() - 2; i++) { // 排除limit和offset参数
                    countStmt.setObject(i + 1, params.get(i));
                }
                try (ResultSet countRs = countStmt.executeQuery()) {
                    if (countRs.next()) {
                        total = countRs.getLong(1);
                    }
                }
            }

            // 获取数据
            List<TableApplicationDTO> tableApplications = new ArrayList<>();
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                for (int i = 0; i < params.size(); i++) {
                    stmt.setObject(i + 1, params.get(i));
                }
                
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        tableApplications.add(mapResultSetToDTO(rs));
                    }
                }
            }

            return new PageResult<>(tableApplications, total, page, size);

        } catch (SQLException e) {
            logger.error("搜索表应用关联失败", e);
            return new PageResult<>(new ArrayList<>(), 0, page, size);
        }
    }

    @Override
    public BatchOperationResult batchDeleteTableApplications(List<Integer> ids) {
        if (ids == null || ids.isEmpty()) {
            return new BatchOperationResult(0, 0, 0, Collections.singletonList("ID列表不能为空"));
        }

        String sql = "DELETE FROM " + TABLE_APPLICATION_TABLE + " WHERE id = ?";
        int success = 0;
        int failed = 0;
        List<String> errors = new ArrayList<>();

        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            for (Integer id : ids) {
                try {
                    stmt.setInt(1, id);
                    int rowsAffected = stmt.executeUpdate();
                    if (rowsAffected > 0) {
                        success++;
                    } else {
                        failed++;
                        errors.add("ID " + id + " 不存在");
                    }
                } catch (SQLException e) {
                    failed++;
                    errors.add("删除ID " + id + " 失败: " + e.getMessage());
                }
            }
            
        } catch (SQLException e) {
            logger.error("批量删除表应用关联失败", e);
            return new BatchOperationResult(0, ids.size(), ids.size(), 
                                          Collections.singletonList("数据库连接失败: " + e.getMessage()));
        }

        return new BatchOperationResult(success, failed, ids.size(), errors);
    }

    @Override
    public boolean existsTableApplicationId(Integer id) {
        String sql = "SELECT COUNT(*) FROM " + TABLE_APPLICATION_TABLE + " WHERE id = ?";
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setInt(1, id);
            
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
            
        } catch (SQLException e) {
            logger.error("检查表应用关联ID是否存在失败: {}", id, e);
            return false;
        }
    }

    @Override
    public List<TableApplicationDTO> getTableApplicationsByTableId(String tableId) {
        String sql = "SELECT * FROM " + TABLE_APPLICATION_TABLE + " WHERE table_id = ? ORDER BY create_time DESC";
        List<TableApplicationDTO> result = new ArrayList<>();
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, tableId);
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    result.add(mapResultSetToDTO(rs));
                }
            }
            
        } catch (SQLException e) {
            logger.error("根据表ID获取表应用关联失败: {}", tableId, e);
        }
        
        return result;
    }

    @Override
    public List<TableApplicationDTO> getTableApplicationsByApplicationName(String applicationName) {
        String sql = "SELECT * FROM " + TABLE_APPLICATION_TABLE + " WHERE application_name = ? ORDER BY create_time DESC";
        List<TableApplicationDTO> result = new ArrayList<>();
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, applicationName);
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    result.add(mapResultSetToDTO(rs));
                }
            }
            
        } catch (SQLException e) {
            logger.error("根据应用名称获取表应用关联失败: {}", applicationName, e);
        }
        
        return result;
    }

    @Override
    public List<TableApplicationDTO> getTableApplicationsByTableType(String tableType) {
        String sql = "SELECT * FROM " + TABLE_APPLICATION_TABLE + " WHERE table_type = ? ORDER BY create_time DESC";
        List<TableApplicationDTO> result = new ArrayList<>();
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, tableType);
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    result.add(mapResultSetToDTO(rs));
                }
            }
            
        } catch (SQLException e) {
            logger.error("根据表类型获取表应用关联失败: {}", tableType, e);
        }
        
        return result;
    }

    @Override
    public boolean existsTableApplicationRelation(String tableId, String applicationName) {
        String sql = "SELECT COUNT(*) FROM " + TABLE_APPLICATION_TABLE + " WHERE table_id = ? AND application_name = ?";
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, tableId);
            stmt.setString(2, applicationName);
            
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
            
        } catch (SQLException e) {
            logger.error("检查表应用关联是否存在失败: tableId={}, applicationName={}", tableId, applicationName, e);
            return false;
        }
    }

    @Override
    public Object getTableApplicationStats() {
        String sql = "SELECT " +
                    "COUNT(*) as total, " +
                    "COUNT(DISTINCT table_id) as total_tables, " +
                    "COUNT(DISTINCT application_name) as total_applications, " +
                    "COUNT(DISTINCT table_type) as total_table_types " +
                    "FROM " + TABLE_APPLICATION_TABLE;
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            
            if (rs.next()) {
                Map<String, Object> result = new HashMap<>();
                result.put("total", rs.getLong("total"));
                result.put("totalTables", rs.getLong("total_tables"));
                result.put("totalApplications", rs.getLong("total_applications"));
                result.put("totalTableTypes", rs.getLong("total_table_types"));
                return result;
            }
            
        } catch (SQLException e) {
            logger.error("获取表应用关联统计信息失败", e);
        }
        
        Map<String, Object> result = new HashMap<>();
        result.put("total", 0);
        result.put("totalTables", 0);
        result.put("totalApplications", 0);
        result.put("totalTableTypes", 0);
        return result;
    }

    @Override
    public List<String> getSupportedTableTypes() {
        return new ArrayList<>(SUPPORTED_TABLE_TYPES);
    }

    /**
     * 将ResultSet映射为TableApplicationDTO
     */
    private TableApplicationDTO mapResultSetToDTO(ResultSet rs) throws SQLException {
        TableApplicationDTO dto = new TableApplicationDTO();
        dto.setId(rs.getInt("id"));
        dto.setTableId(rs.getString("table_id"));
        dto.setApplicationName(rs.getString("application_name"));
        dto.setTableType(rs.getString("table_type"));
        
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
} 