/**
 * API响应基础类型
 */
export interface ApiResponse<T = any> {
  code: number;
  message: string;
  data?: T;
  timestamp: number;
}

/**
 * 分页结果类型
 */
export interface PageResult<T> {
  data: T[];
  total: number;
  page: number;
  size: number;
  hasNext: boolean;
  hasPrevious: boolean;
}

/**
 * Meta Table 数据类型
 */
export interface MetaTableDTO {
  id: string;
  schema: string;
  status: number; // 0-未上线, 1-已上线
  isPartitioned: boolean;
  partitionExpr?: string;
  tags?: string;
  description?: string;
  sourceDb?: string;
  sourceTable?: string;
  targetTable?: string;
  targetDb?: string;
  dbType?: string; // 数据库类型：mysql, oracle, tdsql, buriedpoint等
  hoodieConfig?: string; // Hoodie配置JSON字符串
  createdTime: string;
  updatedTime: string;
  creator?: string;
  updater?: string;
}

/**
 * 创建表请求类型
 */
export interface CreateTableRequest {
  id: string;
  schema: string;
  status: number;
  isPartitioned: boolean;
  partitionExpr?: string;
  tags?: string;
  description?: string;
  sourceDb?: string;
  sourceTable?: string;
  targetTable?: string;
  targetDb?: string;
  dbType?: string; // 数据库类型：mysql, oracle, tdsql, buriedpoint等
  hoodieConfig?: string; // Hoodie配置JSON字符串
}

/**
 * 更新表请求类型
 */
export interface UpdateTableRequest {
  schema?: string;
  status?: number;
  isPartitioned?: boolean;
  partitionExpr?: string;
  tags?: string;
  description?: string;
  sourceDb?: string;
  sourceTable?: string;
  targetTable?: string;
  targetDb?: string;
  dbType?: string; // 数据库类型：mysql, oracle, tdsql, buriedpoint等
  hoodieConfig?: string; // Hoodie配置JSON字符串
}

/**
 * 搜索条件类型
 */
export interface SearchCriteria {
  keyword?: string;
  status?: number;
  isPartitioned?: boolean;
  sourceDb?: string;
  targetDb?: string;
  tags?: string;
  createdTimeStart?: string;
  createdTimeEnd?: string;
}

/**
 * 批量操作请求类型
 */
export interface BatchOperationRequest {
  operation: 'delete' | 'online' | 'offline' | 'export' | 'restore' | 'permanent-delete';
  ids: string[];
  params?: Record<string, any>;
}

/**
 * 批量操作结果类型
 */
export interface BatchOperationResult {
  success: number;
  failed: number;
  total: number;
  errors?: string[];
}

/**
 * 表状态统计类型
 */
export interface TableStatusStats {
  total: number;
  online: number;
  offline: number;
  partitioned: number;
  nonPartitioned: number;
}

/**
 * 表状态枚举
 */
export enum TableStatus {
  OFFLINE = 0,
  ONLINE = 1,
  DELETED = 2,
}

/**
 * 表状态标签映射
 */
export const TableStatusLabels = {
  [TableStatus.OFFLINE]: '未上线',
  [TableStatus.ONLINE]: '已上线',
  [TableStatus.DELETED]: '已删除',
};

/**
 * 表状态颜色映射
 */
export const TableStatusColors = {
  [TableStatus.OFFLINE]: 'default',
  [TableStatus.ONLINE]: 'success',
  [TableStatus.DELETED]: 'error',
} as const;

/**
 * 支持的数据库类型
 */
export const SupportedDbTypes = [
  'mysql',
  'postgresql',
  'oracle',
  'sqlserver',
  'hive',
  'presto',
  'clickhouse',
] as const;

export type DbType = (typeof SupportedDbTypes)[number]; 