package cn.com.multi_writer.service;

import cn.com.multi_writer.service.dto.*;

import java.util.List;
import java.util.Optional;

/**
 * Meta Hudi Table 服务接口
 * 定义所有meta table相关的业务操作
 */
public interface MetaTableService {
    
    /**
     * 获取所有表（分页）
     * @param page 页码（从0开始）
     * @param size 每页大小
     * @return 分页结果
     */
    PageResult<MetaTableDTO> getAllTables(int page, int size);
    
    /**
     * 根据ID获取表详情
     * @param id 表ID
     * @return 表详情，不存在则返回空
     */
    Optional<MetaTableDTO> getTableById(String id);
    
    /**
     * 创建新表
     * @param request 创建表请求
     * @return 创建结果
     */
    boolean createTable(CreateTableRequest request);
    
    /**
     * 更新表信息
     * @param id 表ID
     * @param request 更新请求
     * @return 更新结果
     */
    boolean updateTable(String id, UpdateTableRequest request);
    
    /**
     * 删除表
     * @param id 表ID
     * @return 删除结果
     */
    boolean deleteTable(String id);
    
    /**
     * 高级搜索表
     * @param criteria 搜索条件
     * @param page 页码
     * @param size 每页大小
     * @return 搜索结果
     */
    PageResult<MetaTableDTO> searchTables(SearchCriteria criteria, int page, int size);
    
    /**
     * 批量操作表
     * @param request 批量操作请求
     * @return 操作结果
     */
    BatchOperationResult batchOperation(BatchOperationRequest request);
    
    /**
     * 获取表状态统计
     * @return 状态统计信息
     */
    TableStatusStats getTableStatusStats();
    
    /**
     * 表上线
     * @param id 表ID
     * @return 操作结果
     */
    boolean onlineTable(String id);
    
    /**
     * 表下线
     * @param id 表ID
     * @return 操作结果
     */
    boolean offlineTable(String id);
    
    /**
     * 验证表schema
     * @param schema JSON schema字符串
     * @return 验证结果
     */
    boolean validateTableSchema(String schema);
    
    /**
     * 获取数据库类型列表
     * @return 支持的数据库类型
     */
    List<String> getSupportedDbTypes();
    
    /**
     * 检查表ID是否存在
     * @param id 表ID
     * @return 是否存在
     */
    boolean existsTableId(String id);
    
    /**
     * 批量删除表
     * @param ids 表ID列表
     * @return 批量操作结果
     */
    BatchOperationResult batchDeleteTables(List<String> ids);
    
    /**
     * 批量上线表
     * @param ids 表ID列表
     * @return 批量操作结果
     */
    BatchOperationResult batchOnlineTables(List<String> ids);
    
    /**
     * 批量下线表
     * @param ids 表ID列表
     * @return 批量操作结果
     */
    BatchOperationResult batchOfflineTables(List<String> ids);
    
    /**
     * 导出表数据
     * @param ids 表ID列表
     * @return 批量操作结果
     */
    BatchOperationResult exportTables(List<String> ids);
    
    /**
     * 导出搜索结果
     * @param criteria 搜索条件
     * @return 导出结果
     */
    BatchOperationResult exportSearchResults(SearchCriteria criteria);
    
    /**
     * 获取表Schema信息
     * @param id 表ID
     * @return Schema详情
     */
    Optional<String> getTableSchema(String id);
    
    /**
     * 获取表历史记录
     * @param id 表ID
     * @return 历史记录列表
     */
    List<Object> getTableHistory(String id);
    
    /**
     * 复制表配置
     * @param id 原表ID
     * @param newId 新表ID
     * @return 复制结果
     */
    boolean copyTable(String id, String newId);
    
    /**
     * 获取表使用统计
     * @param id 表ID
     * @return 使用统计
     */
    Object getTableUsageStats(String id);
} 