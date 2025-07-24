package cn.com.multi_writer.service;

import cn.com.multi_writer.service.dto.*;

import java.util.List;
import java.util.Optional;

/**
 * 应用程序服务接口
 * 提供应用程序的CRUD和管理功能
 */
public interface ApplicationService {

    /**
     * 获取所有应用程序（分页）
     * @param page 页码（从0开始）
     * @param size 每页大小
     * @return 分页结果
     */
    PageResult<ApplicationDTO> getAllApplications(int page, int size);

    /**
     * 根据ID获取应用程序详情
     * @param id 应用程序ID
     * @return 应用程序详情，不存在则返回空
     */
    Optional<ApplicationDTO> getApplicationById(Integer id);

    /**
     * 创建新的应用程序
     * @param request 创建请求
     * @return 创建结果
     */
    boolean createApplication(CreateApplicationRequest request);

    /**
     * 更新应用程序信息
     * @param id 应用程序ID
     * @param request 更新请求
     * @return 更新结果
     */
    boolean updateApplication(Integer id, UpdateApplicationRequest request);

    /**
     * 删除应用程序
     * @param id 应用程序ID
     * @return 删除结果
     */
    boolean deleteApplication(Integer id);

    /**
     * 搜索应用程序
     * @param criteria 搜索条件
     * @param page 页码
     * @param size 每页大小
     * @return 搜索结果
     */
    PageResult<ApplicationDTO> searchApplications(ApplicationSearchCriteria criteria, int page, int size);

    /**
     * 批量删除应用程序
     * @param ids 应用程序ID列表
     * @return 批量操作结果
     */
    BatchOperationResult batchDeleteApplications(List<Integer> ids);

    /**
     * 检查应用程序ID是否存在
     * @param id 应用程序ID
     * @return 是否存在
     */
    boolean existsApplicationId(Integer id);

    /**
     * 根据名称获取应用程序
     * @param name 应用程序名称
     * @return 应用程序信息，不存在则返回空
     */
    Optional<ApplicationDTO> getApplicationByName(String name);

    /**
     * 检查应用程序名称是否存在
     * @param name 应用程序名称
     * @return 是否存在
     */
    boolean existsApplicationName(String name);

    /**
     * 获取应用程序统计信息
     * @return 统计信息
     */
    Object getApplicationStats();
} 