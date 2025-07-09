import httpClient from './http';
import {
  ApiResponse,
  PageResult,
  MetaTableDTO,
  CreateTableRequest,
  UpdateTableRequest,
  SearchCriteria,
  BatchOperationRequest,
  BatchOperationResult,
  TableStatusStats,
  DbType,
} from '../types/api';

/**
 * 表管理API服务类
 */
class TableApiService {
  private readonly baseUrl = '/meta/tables';

  /**
   * 获取所有表（分页）
   */
  async getAllTables(
    page: number = 0,
    size: number = 10
  ): Promise<PageResult<MetaTableDTO>> {
    const response = await httpClient.get<PageResult<MetaTableDTO>>(
      `${this.baseUrl}?page=${page}&size=${size}`
    );
    return response.data!;
  }

  /**
   * 根据ID获取表详情
   */
  async getTableById(id: string): Promise<MetaTableDTO> {
    const response = await httpClient.get<MetaTableDTO>(`${this.baseUrl}/${id}`);
    return response.data!;
  }

  /**
   * 创建新表
   */
  async createTable(request: CreateTableRequest): Promise<void> {
    await httpClient.post<void>(this.baseUrl, request);
  }

  /**
   * 更新表信息
   */
  async updateTable(id: string, request: UpdateTableRequest): Promise<void> {
    await httpClient.put<void>(`${this.baseUrl}/${id}`, request);
  }

  /**
   * 删除表
   */
  async deleteTable(id: string): Promise<void> {
    await httpClient.delete<void>(`${this.baseUrl}/${id}`);
  }

  /**
   * 高级搜索表
   */
  async searchTables(
    criteria: SearchCriteria,
    page: number = 0,
    size: number = 10
  ): Promise<PageResult<MetaTableDTO>> {
    const response = await httpClient.post<PageResult<MetaTableDTO>>(
      `${this.baseUrl}/search?page=${page}&size=${size}`,
      criteria
    );
    return response.data!;
  }

  /**
   * 批量操作表
   */
  async batchOperation(
    request: BatchOperationRequest
  ): Promise<BatchOperationResult> {
    const response = await httpClient.post<BatchOperationResult>(
      `${this.baseUrl}/batch`,
      request
    );
    return response.data!;
  }

  /**
   * 获取表状态统计
   */
  async getTableStatusStats(): Promise<TableStatusStats> {
    const response = await httpClient.get<TableStatusStats>(
      `${this.baseUrl}/stats`
    );
    return response.data!;
  }

  /**
   * 表上线
   */
  async onlineTable(id: string): Promise<void> {
    await httpClient.put<void>(`${this.baseUrl}/${id}/online`);
  }

  /**
   * 表下线
   */
  async offlineTable(id: string): Promise<void> {
    await httpClient.put<void>(`${this.baseUrl}/${id}/offline`);
  }

  /**
   * 验证表schema
   */
  async validateTableSchema(schema: string): Promise<void> {
    await httpClient.post<void>(`${this.baseUrl}/validate-schema`, { schema });
  }

  /**
   * 获取支持的数据库类型
   */
  async getSupportedDbTypes(): Promise<DbType[]> {
    const response = await httpClient.get<DbType[]>(`${this.baseUrl}/db-types`);
    return response.data!;
  }

  /**
   * 检查表ID是否存在
   */
  async existsTableId(id: string): Promise<boolean> {
    const response = await httpClient.get<boolean>(
      `${this.baseUrl}/${id}/exists`
    );
    return response.data!;
  }

  /**
   * 批量删除表
   */
  async batchDeleteTables(ids: string[]): Promise<BatchOperationResult> {
    return this.batchOperation({
      operation: 'delete',
      ids,
    });
  }

  /**
   * 批量上线表
   */
  async batchOnlineTables(ids: string[]): Promise<BatchOperationResult> {
    return this.batchOperation({
      operation: 'online',
      ids,
    });
  }

  /**
   * 批量下线表
   */
  async batchOfflineTables(ids: string[]): Promise<BatchOperationResult> {
    return this.batchOperation({
      operation: 'offline',
      ids,
    });
  }

  /**
   * 导出表数据
   */
  async exportTables(ids: string[]): Promise<void> {
    await httpClient.download(
      `${this.baseUrl}/export`,
      'tables.xlsx',
      {
        method: 'POST',
        data: { ids },
      }
    );
  }

  /**
   * 导出搜索结果
   */
  async exportSearchResults(criteria: SearchCriteria): Promise<void> {
    await httpClient.download(
      `${this.baseUrl}/export-search`,
      'search-results.xlsx',
      {
        method: 'POST',
        data: criteria,
      }
    );
  }

  /**
   * 获取表的schema详情
   */
  async getTableSchema(id: string): Promise<any> {
    const response = await httpClient.get<any>(`${this.baseUrl}/${id}/schema`);
    return response.data!;
  }

  /**
   * 获取表的历史版本
   */
  async getTableHistory(id: string): Promise<any[]> {
    const response = await httpClient.get<any[]>(
      `${this.baseUrl}/${id}/history`
    );
    return response.data!;
  }

  /**
   * 复制表配置
   */
  async copyTable(id: string, newId: string): Promise<void> {
    await httpClient.post<void>(`${this.baseUrl}/${id}/copy`, { newId });
  }

  /**
   * 获取表的使用统计
   */
  async getTableUsageStats(id: string): Promise<any> {
    const response = await httpClient.get<any>(`${this.baseUrl}/${id}/usage`);
    return response.data!;
  }

  /**
   * 获取已删除的表列表（分页）
   */
  async getDeletedTables(
    page: number = 0,
    size: number = 10
  ): Promise<PageResult<MetaTableDTO>> {
    const response = await httpClient.get<PageResult<MetaTableDTO>>(
      `${this.baseUrl}/deleted?page=${page}&size=${size}`
    );
    return response.data!;
  }

  /**
   * 搜索已删除的表
   */
  async searchDeletedTables(
    criteria: SearchCriteria,
    page: number = 0,
    size: number = 10
  ): Promise<PageResult<MetaTableDTO>> {
    const response = await httpClient.post<PageResult<MetaTableDTO>>(
      `${this.baseUrl}/deleted/search?page=${page}&size=${size}`,
      criteria
    );
    return response.data!;
  }

  /**
   * 恢复已删除的表
   */
  async restoreTable(id: string): Promise<void> {
    await httpClient.put<void>(`${this.baseUrl}/${id}/restore`);
  }

  /**
   * 批量恢复已删除的表
   */
  async batchRestoreTables(ids: string[]): Promise<BatchOperationResult> {
    return this.batchOperation({
      operation: 'restore',
      ids,
    });
  }

  /**
   * 永久删除表（真正删除）
   */
  async permanentDeleteTable(id: string): Promise<void> {
    await httpClient.delete<void>(`${this.baseUrl}/${id}/permanent`);
  }

  /**
   * 批量永久删除表
   */
  async batchPermanentDeleteTables(ids: string[]): Promise<BatchOperationResult> {
    return this.batchOperation({
      operation: 'permanent-delete',
      ids,
    });
  }

  /**
   * 初始化Hudi元数据表
   */
  async initializeHudiMetaTables(): Promise<void> {
    await httpClient.post<void>(`${this.baseUrl}/init-meta-tables`);
  }

  /**
   * 检查元数据表是否存在
   */
  async checkMetaTablesExist(): Promise<boolean> {
    const response = await httpClient.get<boolean>(`${this.baseUrl}/meta-tables-exist`);
    return response.data!;
  }

  /**
   * 获取元数据表状态
   */
  async getMetaTablesStatus(): Promise<any> {
    const response = await httpClient.get<any>(`${this.baseUrl}/meta-tables-status`);
    return response.data!;
  }

  /**
   * 获取系统表列表
   */
  async getSystemTables(): Promise<any[]> {
    const response = await httpClient.get<any[]>(`${this.baseUrl}/system-tables`);
    return response.data!;
  }

  /**
   * 获取系统表统计信息
   */
  async getSystemTableStats(): Promise<any> {
    const response = await httpClient.get<any>(`${this.baseUrl}/system-table-stats`);
    return response.data!;
  }

  /**
   * 刷新系统表信息
   */
  async refreshSystemTable(tableName: string): Promise<void> {
    // 模拟刷新操作
    return new Promise((resolve) => {
      setTimeout(() => {
        console.log(`刷新系统表: ${tableName}`);
        resolve();
      }, 1000);
    });
  }

  /**
   * 删除所有元数据表（用于重置）
   */
  async dropAllMetaTables(): Promise<void> {
    await httpClient.delete<void>(`${this.baseUrl}/drop-meta-tables`);
  }
}

// 创建并导出表管理API服务实例
export const tableApiService = new TableApiService();
export default tableApiService; 