package cn.com.multi_writer.service;

import cn.com.multi_writer.service.dto.*;

import java.util.List;
import java.util.Optional;

/**
 * 表应用关联服务接口
 * 提供表应用关联的CRUD和管理功能
 */
public interface TableApplicationService {

    /**
     * 获取所有表应用关联（分页）
     * @param page 页码（从0开始）
     * @param size 每页大小
     * @return 分页结果
     */
    PageResult<TableApplicationDTO> getAllTableApplications(int page, int size);

    /**
     * 根据ID获取表应用关联详情
     * @param id 关联ID
     * @return 关联详情，不存在则返回空
     */
    Optional<TableApplicationDTO> getTableApplicationById(Integer id);

    /**
     * 创建新的表应用关联
     * @param request 创建请求
     * @return 创建结果
     */
    boolean createTableApplication(CreateTableApplicationRequest request);

    /**
     * 更新表应用关联信息
     * @param id 关联ID
     * @param request 更新请求
     * @return 更新结果
     */
    boolean updateTableApplication(Integer id, UpdateTableApplicationRequest request);

    /**
     * 删除表应用关联
     * @param id 关联ID
     * @return 删除结果
     */
    boolean deleteTableApplication(Integer id);

    /**
     * 搜索表应用关联
     * @param criteria 搜索条件
     * @param page 页码
     * @param size 每页大小
     * @return 搜索结果
     */
    PageResult<TableApplicationDTO> searchTableApplications(TableApplicationSearchCriteria criteria, int page, int size);

    /**
     * 批量删除表应用关联
     * @param ids 关联ID列表
     * @return 批量操作结果
     */
    BatchOperationResult batchDeleteTableApplications(List<Integer> ids);

    /**
     * 检查表应用关联ID是否存在
     * @param id 关联ID
     * @return 是否存在
     */
    boolean existsTableApplicationId(Integer id);

    /**
     * 根据表ID获取所有关联的应用程序
     * @param tableId 表ID
     * @return 关联的应用程序列表
     */
    List<TableApplicationDTO> getTableApplicationsByTableId(String tableId);

    /**
     * 根据应用名称获取所有关联的表
     * @param applicationName 应用名称
     * @return 关联的表列表
     */
    List<TableApplicationDTO> getTableApplicationsByApplicationName(String applicationName);

    /**
     * 根据表类型获取所有关联
     * @param tableType 表类型
     * @return 关联列表
     */
    List<TableApplicationDTO> getTableApplicationsByTableType(String tableType);

    /**
     * 检查特定表和应用的关联是否存在
     * @param tableId 表ID
     * @param applicationName 应用名称
     * @return 是否存在关联
     */
    boolean existsTableApplicationRelation(String tableId, String applicationName);

    /**
     * 获取表应用关联统计信息
     * @return 统计信息
     */
    Object getTableApplicationStats();

    /**
     * 获取支持的表类型列表
     * @return 表类型列表
     */
    List<String> getSupportedTableTypes();
} 