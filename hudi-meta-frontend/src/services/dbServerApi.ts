import httpClient from './http';
import {
  ApiResponse,
  PageResult,
} from '../types/api';

/**
 * 数据库服务器信息接口
 */
export interface DbServerDTO {
  id: number;
  name: string;
  host: string;
  sourceType: string;
  jdbcUrl: string;
  username: string;
  password: string;
  alias: string;
  description: string;
  createUser: string;
  createTime: string;
  updateUser: string;
  updateTime: string;
}

/**
 * 创建数据库服务器请求
 */
export interface CreateDbServerRequest {
  name: string;
  host: string;
  sourceType: string;
  jdbcUrl: string;
  username: string;
  password: string;
  alias?: string;
  description?: string;
  createUser: string;
}

/**
 * 更新数据库服务器请求
 */
export interface UpdateDbServerRequest {
  name?: string;
  host?: string;
  sourceType?: string;
  jdbcUrl?: string;
  username?: string;
  password?: string;
  alias?: string;
  description?: string;
  updateUser?: string;
}

/**
 * 数据库服务器搜索条件
 */
export interface DbServerSearchCriteria {
  keyword?: string;
  sourceType?: string;
  host?: string;
  createUser?: string;
  createTimeStart?: string;
  createTimeEnd?: string;
}

/**
 * 批量操作结果
 */
export interface BatchOperationResult {
  total: number;
  success: number;
  failed: number;
  errors: string[];
}

/**
 * 业务表信息接口
 */
export interface BusinessTableInfo {
  tableName: string;
  tableComment: string;
  schema: string;
  database: string;
  engine?: string;
  createTime?: string;
  updateTime?: string;
}

/**
 * 表字段信息接口
 */
export interface TableFieldInfo {
  fieldName: string;
  fieldType: string;
  isNullable: boolean;
  defaultValue?: string;
  comment?: string;
  isPrimaryKey?: boolean;
}

/**
 * 表结构信息接口
 */
export interface TableSchemaInfo {
  tableName: string;
  database: string;
  comment?: string;
  fields: TableFieldInfo[];
  primaryKeys: string[];
}

/**
 * 数据库服务器API服务类
 */
class DbServerApiService {
  private readonly baseUrl = '/db-servers';

  /**
   * 获取所有数据库服务器（分页）
   */
  async getAllDbServers(
    page: number = 0,
    size: number = 10
  ): Promise<PageResult<DbServerDTO>> {
    const response = await httpClient.get<PageResult<DbServerDTO>>(
      `${this.baseUrl}?page=${page}&size=${size}`
    );
    return response.data!;
  }

  /**
   * 根据ID获取数据库服务器详情
   */
  async getDbServerById(id: number): Promise<DbServerDTO> {
    const response = await httpClient.get<DbServerDTO>(`${this.baseUrl}/${id}`);
    return response.data!;
  }

  /**
   * 创建新的数据库服务器
   */
  async createDbServer(request: CreateDbServerRequest): Promise<void> {
    await httpClient.post<void>(this.baseUrl, request);
  }

  /**
   * 更新数据库服务器信息
   */
  async updateDbServer(id: number, request: UpdateDbServerRequest): Promise<void> {
    await httpClient.put<void>(`${this.baseUrl}/${id}`, request);
  }

  /**
   * 删除数据库服务器
   */
  async deleteDbServer(id: number): Promise<void> {
    await httpClient.delete<void>(`${this.baseUrl}/${id}`);
  }

  /**
   * 搜索数据库服务器
   */
  async searchDbServers(
    criteria: DbServerSearchCriteria,
    page: number = 0,
    size: number = 10
  ): Promise<PageResult<DbServerDTO>> {
    const response = await httpClient.post<PageResult<DbServerDTO>>(
      `${this.baseUrl}/search?page=${page}&size=${size}`,
      criteria
    );
    return response.data!;
  }

  /**
   * 批量删除数据库服务器
   */
  async batchDeleteDbServers(ids: number[]): Promise<BatchOperationResult> {
    const response = await httpClient.post<BatchOperationResult>(
      `${this.baseUrl}/batch-delete`,
      { ids }
    );
    return response.data!;
  }

  /**
   * 检查数据库服务器ID是否存在
   */
  async existsDbServerId(id: number): Promise<boolean> {
    const response = await httpClient.get<boolean>(`${this.baseUrl}/${id}/exists`);
    return response.data!;
  }

  /**
   * 根据名称获取数据库服务器
   */
  async getDbServerByName(name: string): Promise<DbServerDTO | null> {
    try {
      const response = await httpClient.get<DbServerDTO>(`${this.baseUrl}/by-name/${encodeURIComponent(name)}`);
      return response.data!;
    } catch (error: any) {
      if (error.response?.status === 404) {
        return null;
      }
      throw error;
    }
  }

  /**
   * 根据别名获取数据库服务器
   */
  async getDbServerByAlias(alias: string): Promise<DbServerDTO | null> {
    try {
      const response = await httpClient.get<DbServerDTO>(`${this.baseUrl}/by-alias/${encodeURIComponent(alias)}`);
      return response.data!;
    } catch (error: any) {
      if (error.response?.status === 404) {
        return null;
      }
      throw error;
    }
  }

  /**
   * 测试数据库连接
   */
  async testDbConnection(id: number): Promise<boolean> {
    const response = await httpClient.post<boolean>(`${this.baseUrl}/${id}/test`);
    return response.data!;
  }

  /**
   * 根据数据库类型获取服务器列表
   */
  async getDbServersBySourceType(sourceType: string): Promise<DbServerDTO[]> {
    const response = await httpClient.get<DbServerDTO[]>(`${this.baseUrl}/by-source-type/${encodeURIComponent(sourceType)}`);
    return response.data!;
  }

  /**
   * 获取支持的数据库类型
   */
  async getSupportedSourceTypes(): Promise<string[]> {
    const response = await httpClient.get<string[]>(`${this.baseUrl}/supported-types`);
    return response.data!;
  }

  /**
   * 获取指定数据源的业务表列表
   */
  async getBusinessTables(serverId: number, database?: string): Promise<BusinessTableInfo[]> {
    let url = `${this.baseUrl}/${serverId}/business-tables`;
    if (database) {
      url += `?database=${encodeURIComponent(database)}`;
    }
    const response = await httpClient.get<BusinessTableInfo[]>(url);
    return response.data!;
  }

  /**
   * 获取指定数据源的数据库列表
   */
  async getDatabases(serverId: number): Promise<string[]> {
    const response = await httpClient.get<string[]>(`${this.baseUrl}/${serverId}/databases`);
    return response.data!;
  }

  /**
   * 获取业务表的详细结构信息
   */
  async getTableSchema(serverId: number, database: string, tableName: string): Promise<TableSchemaInfo> {
    const response = await httpClient.get<TableSchemaInfo>(
      `${this.baseUrl}/${serverId}/table-schema?database=${encodeURIComponent(database)}&tableName=${encodeURIComponent(tableName)}`
    );
    return response.data!;
  }

  /**
   * 预览业务表数据（前几行）
   */
  async previewTableData(serverId: number, database: string, tableName: string, limit: number = 10): Promise<any[]> {
    const response = await httpClient.get<any[]>(
      `${this.baseUrl}/${serverId}/table-preview?database=${encodeURIComponent(database)}&tableName=${encodeURIComponent(tableName)}&limit=${limit}`
    );
    return response.data!;
  }
}

// 创建并导出数据库服务器API服务实例
const dbServerApiService = new DbServerApiService();
export default dbServerApiService; 