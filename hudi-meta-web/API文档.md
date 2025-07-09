# Hudi Meta Web API 文档

## 概述

Hudi元数据管理Web层，提供完整的REST API接口。

## 基础信息

- **基础路径**: `/api/meta/tables`
- **内容类型**: `application/json`
- **字符编码**: `UTF-8`

## 表管理接口

### 1. 获取所有表（分页）
- **URL**: `GET /api/meta/tables`
- **参数**:
  - `page`: 页码（从0开始），默认0
  - `size`: 每页大小，默认10
- **响应**: 分页表列表

### 2. 根据ID获取表详情
- **URL**: `GET /api/meta/tables/{id}`
- **参数**: 
  - `id`: 表ID
- **响应**: 表详情信息

### 3. 创建新表
- **URL**: `POST /api/meta/tables`
- **请求体**: CreateTableRequest对象
- **响应**: 创建结果

### 4. 更新表信息
- **URL**: `PUT /api/meta/tables/{id}`
- **参数**: 
  - `id`: 表ID
- **请求体**: UpdateTableRequest对象
- **响应**: 更新结果

### 5. 删除表
- **URL**: `DELETE /api/meta/tables/{id}`
- **参数**: 
  - `id`: 表ID
- **响应**: 删除结果

### 6. 高级搜索
- **URL**: `POST /api/meta/tables/search`
- **参数**:
  - `page`: 页码
  - `size`: 每页大小
- **请求体**: SearchCriteria对象
- **响应**: 搜索结果

### 7. 批量操作
- **URL**: `POST /api/meta/tables/batch`
- **请求体**: BatchOperationRequest对象
- **响应**: 批量操作结果

### 8. 获取表状态统计
- **URL**: `GET /api/meta/tables/stats`
- **响应**: 状态统计信息

### 9. 表上线
- **URL**: `POST /api/meta/tables/{id}/online`
- **参数**: 
  - `id`: 表ID
- **响应**: 操作结果

### 10. 表下线
- **URL**: `POST /api/meta/tables/{id}/offline`
- **参数**: 
  - `id`: 表ID
- **响应**: 操作结果

## 健康检查接口

### 1. 基础健康检查
- **URL**: `GET /api/health`
- **响应**: 健康状态信息

### 2. 详细健康检查
- **URL**: `GET /api/health/detail`
- **响应**: 详细健康状态信息

## 响应格式

```json
{
  "code": 200,
  "message": "操作成功",
  "data": {},
  "timestamp": "2023-12-01 10:00:00"
}
```

## 错误码

- 200: 成功
- 201: 创建成功
- 400: 请求参数错误
- 404: 资源不存在
- 409: 资源冲突
- 500: 服务器内部错误 