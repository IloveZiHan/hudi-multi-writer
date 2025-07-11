import httpClient from './http';
import { ApiResponse } from '../types/api';

/**
 * Hoodie配置API服务类
 */
class HoodieConfigApiService {
  private readonly baseUrl = '/hoodie-config';

  /**
   * 获取默认的Hoodie配置
   */
  async getDefaultConfig(): Promise<string> {
    const response = await httpClient.get<string>(`${this.baseUrl}/get-default-config`);
    return response.data!;
  }
}

// 创建并导出Hoodie配置API服务实例
export const hoodieConfigApiService = new HoodieConfigApiService();
export default hoodieConfigApiService; 