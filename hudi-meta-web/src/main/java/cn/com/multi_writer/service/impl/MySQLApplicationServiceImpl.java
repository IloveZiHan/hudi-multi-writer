package cn.com.multi_writer.service.impl;

import cn.com.multi_writer.service.ApplicationService;
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
 * MySQL实现的应用程序服务
 */
@Service("mySQLApplicationService")
public class MySQLApplicationServiceImpl implements ApplicationService {

    private static final Logger logger = LoggerFactory.getLogger(MySQLApplicationServiceImpl.class);

    @Autowired
    private DataSource dataSource;

    // 表名常量
    private static final String APPLICATION_TABLE = "hoodie_meta_application";

    private Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    @Override
    public PageResult<ApplicationDTO> getAllApplications(int page, int size) {
        String sql = "SELECT * FROM " + APPLICATION_TABLE + " ORDER BY create_time DESC LIMIT ? OFFSET ?";
        String countSql = "SELECT COUNT(*) FROM " + APPLICATION_TABLE;

        logger.info(sql);

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
            List<ApplicationDTO> applications = new ArrayList<>();
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setInt(1, size);
                stmt.setInt(2, page * size);
                
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        applications.add(mapResultSetToDTO(rs));
                    }
                }
            }

            return new PageResult<>(applications, total, page, size);

        } catch (SQLException e) {
            logger.error("获取所有应用程序失败", e);
            return new PageResult<>(new ArrayList<>(), 0, page, size);
        }
    }

    @Override
    public Optional<ApplicationDTO> getApplicationById(Integer id) {
        String sql = "SELECT * FROM " + APPLICATION_TABLE + " WHERE id = ?";
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setInt(1, id);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapResultSetToDTO(rs));
                }
            }
            
        } catch (SQLException e) {
            logger.error("根据ID获取应用程序失败: {}", id, e);
        }
        
        return Optional.empty();
    }

    @Override
    public boolean createApplication(CreateApplicationRequest request) {
        String sql = "INSERT INTO " + APPLICATION_TABLE + " (name, description, conf) VALUES (?, ?, ?)";
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, request.getName());
            stmt.setString(2, request.getDescription());
            stmt.setString(3, request.getConf());
            
            int rowsAffected = stmt.executeUpdate();
            logger.info("创建应用程序成功: {}", request.getName());
            return rowsAffected > 0;
            
        } catch (SQLException e) {
            logger.error("创建应用程序失败: {}", request.getName(), e);
            return false;
        }
    }

    @Override
    public boolean updateApplication(Integer id, UpdateApplicationRequest request) {
        List<String> setClauses = new ArrayList<>();
        List<Object> params = new ArrayList<>();
        
        if (request.getName() != null) {
            setClauses.add("name = ?");
            params.add(request.getName());
        }
        if (request.getDescription() != null) {
            setClauses.add("description = ?");
            params.add(request.getDescription());
        }
        if (request.getConf() != null) {
            setClauses.add("conf = ?");
            params.add(request.getConf());
        }
        
        if (setClauses.isEmpty()) {
            return true; // 没有需要更新的字段
        }
        
        setClauses.add("update_time = CURRENT_TIMESTAMP");
        params.add(id);
        
        String sql = "UPDATE " + APPLICATION_TABLE + " SET " + String.join(", ", setClauses) + " WHERE id = ?";
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            for (int i = 0; i < params.size(); i++) {
                stmt.setObject(i + 1, params.get(i));
            }
            
            int rowsAffected = stmt.executeUpdate();
            logger.info("更新应用程序成功: ID={}", id);
            return rowsAffected > 0;
            
        } catch (SQLException e) {
            logger.error("更新应用程序失败: ID={}", id, e);
            return false;
        }
    }

    @Override
    public boolean deleteApplication(Integer id) {
        String sql = "DELETE FROM " + APPLICATION_TABLE + " WHERE id = ?";
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setInt(1, id);
            
            int rowsAffected = stmt.executeUpdate();
            logger.info("删除应用程序成功: ID={}", id);
            return rowsAffected > 0;
            
        } catch (SQLException e) {
            logger.error("删除应用程序失败: ID={}", id, e);
            return false;
        }
    }

    @Override
    public PageResult<ApplicationDTO> searchApplications(ApplicationSearchCriteria criteria, int page, int size) {
        List<String> conditions = new ArrayList<>();
        List<Object> params = new ArrayList<>();
        
        if (criteria.getKeyword() != null && !criteria.getKeyword().trim().isEmpty()) {
            conditions.add("(name LIKE ? OR description LIKE ?)");
            String keyword = "%" + criteria.getKeyword().trim() + "%";
            params.add(keyword);
            params.add(keyword);
        }
        
        if (criteria.getName() != null && !criteria.getName().trim().isEmpty()) {
            conditions.add("name LIKE ?");
            params.add("%" + criteria.getName().trim() + "%");
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
        String sql = "SELECT * FROM " + APPLICATION_TABLE + whereClause + " ORDER BY create_time DESC LIMIT ? OFFSET ?";
        String countSql = "SELECT COUNT(*) FROM " + APPLICATION_TABLE + whereClause;
        
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
            List<ApplicationDTO> applications = new ArrayList<>();
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                for (int i = 0; i < params.size(); i++) {
                    stmt.setObject(i + 1, params.get(i));
                }
                
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        applications.add(mapResultSetToDTO(rs));
                    }
                }
            }

            return new PageResult<>(applications, total, page, size);

        } catch (SQLException e) {
            logger.error("搜索应用程序失败", e);
            return new PageResult<>(new ArrayList<>(), 0, page, size);
        }
    }

    @Override
    public BatchOperationResult batchDeleteApplications(List<Integer> ids) {
        if (ids == null || ids.isEmpty()) {
            return new BatchOperationResult(0, 0, 0, Collections.singletonList("ID列表不能为空"));
        }

        String sql = "DELETE FROM " + APPLICATION_TABLE + " WHERE id = ?";
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
            logger.error("批量删除应用程序失败", e);
            return new BatchOperationResult(0, ids.size(), ids.size(), Collections.singletonList("数据库连接失败: " + e.getMessage()));
        }

        return new BatchOperationResult(success, failed, ids.size(), errors);
    }

    @Override
    public boolean existsApplicationId(Integer id) {
        String sql = "SELECT COUNT(*) FROM " + APPLICATION_TABLE + " WHERE id = ?";
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setInt(1, id);
            
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
            
        } catch (SQLException e) {
            logger.error("检查应用程序ID是否存在失败: {}", id, e);
            return false;
        }
    }

    @Override
    public Optional<ApplicationDTO> getApplicationByName(String name) {
        String sql = "SELECT * FROM " + APPLICATION_TABLE + " WHERE name = ?";
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, name);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapResultSetToDTO(rs));
                }
            }
            
        } catch (SQLException e) {
            logger.error("根据名称获取应用程序失败: {}", name, e);
        }
        
        return Optional.empty();
    }

    @Override
    public boolean existsApplicationName(String name) {
        String sql = "SELECT COUNT(*) FROM " + APPLICATION_TABLE + " WHERE name = ?";
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, name);
            
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
            
        } catch (SQLException e) {
            logger.error("检查应用程序名称是否存在失败: {}", name, e);
            return false;
        }
    }

    @Override
    public Object getApplicationStats() {
        String sql = "SELECT COUNT(*) as total FROM " + APPLICATION_TABLE;
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            
            if (rs.next()) {
                Map<String, Object> result = new HashMap<>();
                result.put("total", rs.getLong("total"));
                return result;
            }
            
        } catch (SQLException e) {
            logger.error("获取应用程序统计信息失败", e);
        }
        
        Map<String, Object> result = new HashMap<>();
        result.put("total", 0);
        return result;
    }

    /**
     * 将ResultSet映射为ApplicationDTO
     */
    private ApplicationDTO mapResultSetToDTO(ResultSet rs) throws SQLException {
        ApplicationDTO dto = new ApplicationDTO();
        dto.setId(rs.getInt("id"));
        dto.setName(rs.getString("name"));
        dto.setDescription(rs.getString("description"));
        dto.setConf(rs.getString("conf"));
        
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