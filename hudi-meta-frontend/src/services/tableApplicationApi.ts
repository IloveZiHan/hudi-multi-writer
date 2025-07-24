import httpClient from './http';
import {
  ApiResponse,
  PageResult,
} from '../types/api';

// 表应用关联DTO接口
export interface TableApplicationDTO {
  id: number;
  tableId: string;
  applicationName: string;
  tableType: string;
  createTime: string;
  updateTime: string;
}

// 创建表应用关联请求接口
export interface CreateTableApplicationRequest {
  tableId: string;
  applicationName: string;
  tableType: string;
}

// 更新表应用关联请求接口
export interface UpdateTableApplicationRequest {
  tableId?: string;
  applicationName?: string;
  tableType?: string;
}

// 表应用关联搜索条件接口
export interface TableApplicationSearchCriteria {
  keyword?: string;
  tableId?: string;
  applicationName?: string;
  tableType?: string;
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
 * 表应用关联API服务类
 */
class TableApplicationApiService {
  private readonly baseUrl = '/table-applications';

  /**
   * 获取所有表应用关联（分页）
   */
  async getAllTableApplications(
    page: number = 0,
    size: number = 10
  ): Promise<PageResult<TableApplicationDTO>> {
    const response = await httpClient.get<PageResult<TableApplicationDTO>>(
      `${this.baseUrl}?page=${page}&size=${size}`
    );
    return response.data!;
  }

  /**
   * 根据ID获取表应用关联详情
   */
  async getTableApplicationById(id: number): Promise<TableApplicationDTO> {
    const response = await httpClient.get<TableApplicationDTO>(`/table-applications/${id}`);
    return response.data!;
  }

  /**
   * 创建新的表应用关联
   */
  async createTableApplication(request: CreateTableApplicationRequest): Promise<void> {
    await httpClient.post<void>('/table-applications', request);
  }

  /**
   * 更新表应用关联信息
   */
  async updateTableApplication(id: number, request: UpdateTableApplicationRequest): Promise<void> {
    await httpClient.put<void>(`/table-applications/${id}`, request);
  }

  /**
   * 删除表应用关联
   */
  async deleteTableApplication(id: number): Promise<void> {
    await httpClient.delete<void>(`/table-applications/${id}`);
  }

  /**
   * 搜索表应用关联
   */
  async searchTableApplications(
    criteria: TableApplicationSearchCriteria,
    page: number = 0,
    size: number = 10
  ): Promise<PageResult<TableApplicationDTO>> {
    const response = await httpClient.post<PageResult<TableApplicationDTO>>(
      `${this.baseUrl}/search?page=${page}&size=${size}`,
      criteria
    );
    return response.data!;
  }

  /**
   * 批量删除表应用关联
   */
  async batchDeleteTableApplications(ids: number[]): Promise<BatchOperationResult> {
    const response = await httpClient.delete<BatchOperationResult>(
      `${this.baseUrl}/batch`,
      {
        data: ids,
      }
    );
    return response.data!;
  }

  /**
   * 检查表应用关联ID是否存在
   */
  async existsTableApplicationId(id: number): Promise<boolean> {
    const response = await httpClient.get<boolean>(`${this.baseUrl}/${id}/exists`);
    return response.data!;
  }

  /**
   * 根据表ID获取所有关联的应用程序
   */
  async getTableApplicationsByTableId(tableId: string): Promise<TableApplicationDTO[]> {
    const response = await httpClient.get<TableApplicationDTO[]>(`${this.baseUrl}/table/${tableId}`);
    return response.data!;
  }

  /**
   * 根据应用名称获取所有关联的表
   */
  async getTableApplicationsByApplicationName(applicationName: string): Promise<TableApplicationDTO[]> {
    const response = await httpClient.get<TableApplicationDTO[]>(`${this.baseUrl}/application/${applicationName}`);
    return response.data!;
  }

  /**
   * 根据表类型获取所有关联
   */
  async getTableApplicationsByTableType(tableType: string): Promise<TableApplicationDTO[]> {
    const response = await httpClient.get<TableApplicationDTO[]>(`${this.baseUrl}/type/${tableType}`);
    return response.data!;
  }

  /**
   * 检查特定表和应用的关联是否存在
   */
  async existsTableApplicationRelation(tableId: string, applicationName: string): Promise<boolean> {
    const response = await httpClient.get<boolean>(
      `${this.baseUrl}/exists?tableId=${tableId}&applicationName=${applicationName}`
    );
    return response.data!;
  }

  /**
   * 获取表应用关联统计信息
   */
  async getTableApplicationStats(): Promise<any> {
    const response = await httpClient.get<any>(`${this.baseUrl}/stats`);
    return response.data!;
  }

  /**
   * 获取支持的表类型列表
   */
  async getSupportedTableTypes(): Promise<string[]> {
    const response = await httpClient.get<string[]>(`${this.baseUrl}/supported-types`);
    return response.data!;
  }
}

const tableApplicationApiService = new TableApplicationApiService();
export default tableApplicationApiService; 