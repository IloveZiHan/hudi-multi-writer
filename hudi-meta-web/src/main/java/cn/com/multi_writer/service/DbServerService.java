package cn.com.multi_writer.service;

import cn.com.multi_writer.service.dto.*;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 数据库服务器服务接口
 * 定义所有数据库服务器相关的业务操作
 */
public interface DbServerService {

    /**
     * 获取所有数据库服务器（分页）
     * @param page 页码（从0开始）
     * @param size 每页大小
     * @return 分页结果
     */
    PageResult<DbServerDTO> getAllDbServers(int page, int size);

    /**
     * 根据ID获取数据库服务器详情
     * @param id 服务器ID
     * @return 服务器详情，不存在则返回空
     */
    Optional<DbServerDTO> getDbServerById(Integer id);

    /**
     * 创建新的数据库服务器
     * @param request 创建请求
     * @return 创建结果
     */
    boolean createDbServer(CreateDbServerRequest request);

    /**
     * 更新数据库服务器信息
     * @param id 服务器ID
     * @param request 更新请求
     * @return 更新结果
     */
    boolean updateDbServer(Integer id, UpdateDbServerRequest request);

    /**
     * 删除数据库服务器
     * @param id 服务器ID
     * @return 删除结果
     */
    boolean deleteDbServer(Integer id);

    /**
     * 搜索数据库服务器
     * @param criteria 搜索条件
     * @param page 页码
     * @param size 每页大小
     * @return 搜索结果
     */
    PageResult<DbServerDTO> searchDbServers(DbServerSearchCriteria criteria, int page, int size);

    /**
     * 批量删除数据库服务器
     * @param ids 服务器ID列表
     * @return 批量操作结果
     */
    BatchOperationResult batchDeleteDbServers(List<Integer> ids);

    /**
     * 检查数据库服务器ID是否存在
     * @param id 服务器ID
     * @return 是否存在
     */
    boolean existsDbServerId(Integer id);

    /**
     * 根据名称获取数据库服务器
     * @param name 服务器名称
     * @return 服务器信息，不存在则返回空
     */
    Optional<DbServerDTO> getDbServerByName(String name);

    /**
     * 根据别名获取数据库服务器
     * @param alias 服务器别名
     * @return 服务器信息，不存在则返回空
     */
    Optional<DbServerDTO> getDbServerByAlias(String alias);

    /**
     * 测试数据库连接
     * @param id 服务器ID
     * @return 连接测试结果
     */
    boolean testDbConnection(Integer id);

    /**
     * 根据数据库类型获取服务器列表
     * @param sourceType 数据库类型
     * @return 服务器列表
     */
    List<DbServerDTO> getDbServersBySourceType(String sourceType);

    /**
     * 获取支持的数据库类型列表
     * @return 数据库类型列表
     */
    List<String> getSupportedSourceTypes();

    /**
     * 获取数据库列表
     * @param id 服务器ID
     * @return 数据库列表
     */
    List<String> getDatabases(Integer id) throws SQLException;

    /**
     * 获取业务表列表
     * @param id 服务器ID
     * @param database 数据库名
     * @return 业务表列表
     */
    List<BusinessTableInfoDTO> getBusinessTables(Integer id, String database) throws SQLException;

    /**
     * 获取表结构
     * @param id 服务器ID
     * @param database 数据库名
     * @param tableName 表名
     * @return 表结构信息
     */
    TableSchemaInfoDTO getTableSchema(Integer id, String database, String tableName) throws SQLException;

    /**
     * 预览表数据（前几行）
     * @param id 服务器ID
     * @param database 数据库名
     * @param tableName 表名
     * @param limit 限制行数
     * @return 表数据预览
     */
    List<Map<String, Object>> previewTableData(Integer id, String database, String tableName, int limit) throws SQLException;
} 