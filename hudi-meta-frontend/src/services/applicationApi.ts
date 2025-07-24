import httpClient from './http';
import {
  ApiResponse,
  PageResult,
} from '../types/api';

// 应用程序DTO接口
export interface ApplicationDTO {
  id: number;
  name: string;
  description?: string;
  conf?: string;
  createTime: string;
  updateTime: string;
}

// 创建应用程序请求接口
export interface CreateApplicationRequest {
  name: string;
  description?: string;
  conf?: string;
}

// 更新应用程序请求接口
export interface UpdateApplicationRequest {
  name?: string;
  description?: string;
  conf?: string;
}

// 应用程序搜索条件接口
export interface ApplicationSearchCriteria {
  keyword?: string;
  name?: string;
  createTimeStart?: string;
  createTimeEnd?: string;
}

// 批量操作结果接口
export interface BatchOperationResult {
  total: number;
  success: number;
  failed: number;
  errors: string[];
}

/**
 * 应用程序API服务类
 */
class ApplicationApiService {
  private readonly baseUrl = '/applications';

  constructor() {
    // 确保baseUrl正确初始化
    if (!this.baseUrl) {
      throw new Error('ApplicationApiService baseUrl 未正确初始化');
    }
    console.log('ApplicationApiService initialized with baseUrl:', this.baseUrl);
  }

  /**
   * 获取所有应用程序（分页）
   */
  async getAllApplications(
    page: number = 0,
    size: number = 10
  ): Promise<PageResult<ApplicationDTO>> {
    const response = await httpClient.get<PageResult<ApplicationDTO>>(
      `/applications?page=${page}&size=${size}`
    );
    return response.data!;
  }

  /**
   * 根据ID获取应用程序详情
   */
  async getApplicationById(id: number): Promise<ApplicationDTO> {
    const response = await httpClient.get<ApplicationDTO>(`/applications/${id}`);
    return response.data!;
  }

  /**
   * 创建新的应用程序
   */
  async createApplication(request: CreateApplicationRequest): Promise<void> {
    console.log('createApplication - baseUrl:', this.baseUrl);
    console.log('createApplication - request:', request);
    
    // 直接使用硬编码的路径以确保正确性
    const url = '/applications';
    console.log('createApplication - final url:', url);
    
    await httpClient.post<void>(url, request);
  }

  /**
   * 更新应用程序信息
   */
  async updateApplication(id: number, request: UpdateApplicationRequest): Promise<void> {
    await httpClient.put<void>(`/applications/${id}`, request);
  }

  /**
   * 删除应用程序
   */
  async deleteApplication(id: number): Promise<void> {
    await httpClient.delete<void>(`/applications/${id}`);
  }

  /**
   * 搜索应用程序
   */
  async searchApplications(
    criteria: ApplicationSearchCriteria,
    page: number = 0,
    size: number = 10
  ): Promise<PageResult<ApplicationDTO>> {
    const response = await httpClient.post<PageResult<ApplicationDTO>>(
      `/applications/search?page=${page}&size=${size}`,
      criteria
    );
    return response.data!;
  }

  /**
   * 批量删除应用程序
   */
  async batchDeleteApplications(ids: number[]): Promise<BatchOperationResult> {
    const response = await httpClient.delete<BatchOperationResult>(
      `/applications/batch`,
      {
        data: ids,
      }
    );
    return response.data!;
  }

  /**
   * 检查应用程序ID是否存在
   */
  async existsApplicationId(id: number): Promise<boolean> {
    const response = await httpClient.get<boolean>(`/applications/${id}/exists`);
    return response.data!;
  }

  /**
   * 根据名称获取应用程序
   */
  async getApplicationByName(name: string): Promise<ApplicationDTO | null> {
    try {
      const response = await httpClient.get<ApplicationDTO>(`/applications/name/${name}`);
      return response.data!;
    } catch (error) {
      // 如果不存在，返回null
      return null;
    }
  }

  /**
   * 检查应用程序名称是否存在
   */
  async existsApplicationName(name: string): Promise<boolean> {
    const response = await httpClient.get<boolean>(`/applications/name/${name}/exists`);
    return response.data!;
  }

  /**
   * 获取应用程序统计信息
   */
  async getApplicationStats(): Promise<any> {
    const response = await httpClient.get<any>(`/applications/stats`);
    return response.data!;
  }
}

const applicationApiService = new ApplicationApiService();
export default applicationApiService; 