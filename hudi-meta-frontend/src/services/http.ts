import axios, { AxiosInstance, AxiosRequestConfig, AxiosResponse } from 'axios';
import { message } from 'antd';
import { ApiResponse } from '@types/api';

/**
 * HTTP客户端类
 */
class HttpClient {
  private instance: AxiosInstance;

  constructor() {
    this.instance = axios.create({
      baseURL: '/api',
      timeout: 30000,
      headers: {
        'Content-Type': 'application/json',
      },
    });

    this.setupInterceptors();
  }

  /**
   * 设置请求和响应拦截器
   */
  private setupInterceptors() {
    // 请求拦截器
    this.instance.interceptors.request.use(
      (config) => {
        // 在请求发送前做一些处理
        console.log('发送请求:', config.method?.toUpperCase(), config.url);
        
        // 添加认证token（如果有）
        const token = localStorage.getItem('token');
        if (token) {
          config.headers.Authorization = `Bearer ${token}`;
        }

        return config;
      },
      (error) => {
        console.error('请求错误:', error);
        return Promise.reject(error);
      }
    );

    // 响应拦截器
    this.instance.interceptors.response.use(
      (response: AxiosResponse<ApiResponse>) => {
        const { data } = response;
        
        // 如果是成功状态码，直接返回数据
        if (data.code === 200 || data.code === 201) {
          return response;
        }
        
        // 处理业务错误
        const errorMsg = data.message || '请求失败';
        message.error(errorMsg);
        return Promise.reject(new Error(errorMsg));
      },
      (error) => {
        console.error('响应错误:', error);
        
        // 处理HTTP错误
        if (error.response) {
          const { status, data } = error.response;
          
          switch (status) {
            case 400:
              message.error(data?.message || '请求参数错误');
              break;
            case 401:
              message.error('未授权，请重新登录');
              // 清除token并跳转到登录页
              localStorage.removeItem('token');
              window.location.href = '/login';
              break;
            case 403:
              message.error('禁止访问');
              break;
            case 404:
              message.error('资源不存在');
              break;
            case 409:
              message.error(data?.message || '资源冲突');
              break;
            case 422:
              message.error(data?.message || '数据验证失败');
              break;
            case 500:
              message.error('服务器内部错误');
              break;
            default:
              message.error(`请求失败: ${status}`);
          }
        } else if (error.request) {
          message.error('网络错误，请检查网络连接');
        } else {
          message.error('请求配置错误');
        }
        
        return Promise.reject(error);
      }
    );
  }

  /**
   * GET请求
   */
  public get<T = any>(
    url: string,
    config?: AxiosRequestConfig
  ): Promise<ApiResponse<T>> {
    return this.instance.get(url, config).then(response => response.data);
  }

  /**
   * POST请求
   */
  public post<T = any>(
    url: string,
    data?: any,
    config?: AxiosRequestConfig
  ): Promise<ApiResponse<T>> {
    return this.instance.post(url, data, config).then(response => response.data);
  }

  /**
   * PUT请求
   */
  public put<T = any>(
    url: string,
    data?: any,
    config?: AxiosRequestConfig
  ): Promise<ApiResponse<T>> {
    return this.instance.put(url, data, config).then(response => response.data);
  }

  /**
   * DELETE请求
   */
  public delete<T = any>(
    url: string,
    config?: AxiosRequestConfig
  ): Promise<ApiResponse<T>> {
    return this.instance.delete(url, config).then(response => response.data);
  }

  /**
   * PATCH请求
   */
  public patch<T = any>(
    url: string,
    data?: any,
    config?: AxiosRequestConfig
  ): Promise<ApiResponse<T>> {
    return this.instance.patch(url, data, config).then(response => response.data);
  }

  /**
   * 上传文件
   */
  public upload<T = any>(
    url: string,
    file: File,
    config?: AxiosRequestConfig
  ): Promise<ApiResponse<T>> {
    const formData = new FormData();
    formData.append('file', file);
    
    return this.instance.post(url, formData, {
      ...config,
      headers: {
        'Content-Type': 'multipart/form-data',
        ...config?.headers,
      },
    }).then(response => response.data);
  }

  /**
   * 下载文件
   */
  public download(
    url: string,
    filename?: string,
    config?: AxiosRequestConfig
  ): Promise<void> {
    return this.instance.get(url, {
      ...config,
      responseType: 'blob',
    }).then(response => {
      const blob = new Blob([response.data]);
      const downloadUrl = window.URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = downloadUrl;
      link.download = filename || 'download';
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      window.URL.revokeObjectURL(downloadUrl);
    });
  }

  /**
   * 获取原始axios实例
   */
  public getInstance(): AxiosInstance {
    return this.instance;
  }
}

// 创建并导出HTTP客户端实例
export const httpClient = new HttpClient();
export default httpClient; 