# Hudi Multi Writer

一个基于Apache Hudi的多写入器项目，现在包含完整的Web管理系统。

## 项目结构

```
hudi-multi-writer/
├── spark-hudi-multi-writer/    # 原有的Spark多写入器模块
├── hudi-meta-service/          # 新增：业务服务层
├── hudi-meta-web/              # 新增：Web API层
├── hudi-meta-frontend/         # 新增：前端管理界面
├── hudi-meta-service/          # 元数据服务
├── hudi-meta-web/              # Web服务
├── pom.xml                     # Maven父项目配置
└── README.md                   # 项目说明文档
```

## 🚀 新增功能

### 1. Web管理系统

- **前端界面**：基于React + TypeScript + Ant Design
- **后端API**：基于Spring Boot + Scala
- **功能特性**：
  - 表的增删改查管理
  - 批量操作（上线、下线、删除）
  - 高级搜索和过滤
  - 数据统计和可视化
  - 表详情查看和编辑
  - 数据导出功能

### 2. 技术栈

#### 后端
- **Spring Boot 2.7.14**：Web框架
- **Scala 2.12.18**：编程语言
- **Apache Spark 3.5.0**：大数据处理
- **Apache Hudi 0.15.0**：湖仓一体化存储
- **Jackson**：JSON序列化
- **Bean Validation**：数据验证

#### 前端
- **React 18.2.0**：前端框架
- **TypeScript 5.2.2**：类型安全
- **Ant Design 5.12.1**：UI组件库
- **Vite 5.0.0**：构建工具
- **Axios 1.6.2**：HTTP客户端
- **React Router 6.20.1**：路由管理
- **Dayjs 1.11.10**：时间处理

## 🛠️ 快速开始

### 环境要求

- Java 8+
- Maven 3.6+
- Node.js 16+
- npm 7+

### 1. 后端启动

```bash
# 进入项目根目录
cd hudi-multi-writer

# 编译整个项目
mvn clean compile

# 启动Web服务
cd hudi-meta-web
mvn spring-boot:run
```

服务启动后，访问：`http://localhost:8080`

### 2. 前端启动

```bash
# 进入前端目录
cd hudi-meta-frontend

# 安装依赖
npm install

# 启动开发服务器
npm run dev
```

前端启动后，访问：`http://localhost:3000`

### 3. 生产环境部署

#### 后端部署

```bash
# 构建JAR包
mvn clean package -DskipTests

# 运行JAR包
java -jar hudi-meta-web/target/hudi-meta-web-1.0.0.jar --spring.profiles.active=prod
```

#### 前端部署

```bash
# 构建生产版本
npm run build

# 部署到Web服务器
# 将dist目录的内容部署到nginx或其他Web服务器
```

## 📚 API文档

### 主要API端点

| 方法 | 路径 | 描述 |
|------|------|------|
| GET | `/api/v1/tables` | 获取所有表（分页） |
| GET | `/api/v1/tables/{id}` | 获取表详情 |
| POST | `/api/v1/tables` | 创建新表 |
| PUT | `/api/v1/tables/{id}` | 更新表信息 |
| DELETE | `/api/v1/tables/{id}` | 删除表 |
| POST | `/api/v1/tables/search` | 高级搜索 |
| POST | `/api/v1/tables/batch` | 批量操作 |
| GET | `/api/v1/tables/stats` | 获取统计信息 |
| PUT | `/api/v1/tables/{id}/online` | 表上线 |
| PUT | `/api/v1/tables/{id}/offline` | 表下线 |

### 请求示例

```bash
# 获取所有表
curl -X GET "http://localhost:8080/api/v1/tables?page=0&size=10"

# 创建新表
curl -X POST "http://localhost:8080/api/v1/tables" \
  -H "Content-Type: application/json" \
  -d '{
    "id": "test_table",
    "schema": "CREATE TABLE test_table (id INT, name STRING)",
    "status": 0,
    "isPartitioned": false
  }'
```

## 🔧 配置说明

### 后端配置

主要配置文件：`hudi-meta-web/src/main/resources/application.yml`

```yaml
# 服务端口
server:
  port: 8080

# Spark配置
spark:
  app:
    name: "Hudi Meta Web"
  master: "local[*]"

# Hudi配置
hudi:
  meta:
    table:
      path: "/tmp/hudi_tables/meta_hudi_table"
```

### 前端配置

主要配置文件：`hudi-meta-frontend/vite.config.ts`

```typescript
// API代理配置
proxy: {
  '/api': {
    target: 'http://localhost:8080',
    changeOrigin: true,
    secure: false,
  },
}
```

## 🎯 功能特性

### 1. 表管理功能

- ✅ 创建表：支持自定义schema和配置
- ✅ 查看表：详细信息展示和列表查看
- ✅ 编辑表：修改表配置和属性
- ✅ 删除表：安全删除确认
- ✅ 批量操作：批量删除、上线、下线
- ✅ 搜索过滤：多条件搜索和高级过滤

### 2. 数据统计

- ✅ 表数量统计
- ✅ 状态分布统计
- ✅ 分区表统计
- ✅ 实时数据更新

### 3. 用户体验

- ✅ 响应式设计
- ✅ 国际化支持（中文）
- ✅ 错误处理和用户提示
- ✅ 加载状态和进度提示
- ✅ 操作确认和回滚

## 🐛 问题排查

### 常见问题

1. **后端启动失败**
   - 检查Java版本和Maven配置
   - 确认端口8080未被占用
   - 查看日志文件：`logs/hudi-meta-web.log`

2. **前端无法访问后端**
   - 检查代理配置
   - 确认后端服务已启动
   - 检查网络连接

3. **数据不显示**
   - 检查Spark配置
   - 确认Hudi表路径正确
   - 查看后端日志

### 日志查看

```bash
# 后端日志
tail -f logs/hudi-meta-web.log

# 前端控制台
# 打开浏览器开发者工具查看Console
```

## 🤝 贡献指南

1. Fork项目
2. 创建功能分支
3. 提交更改
4. 推送到分支
5. 创建Pull Request

## 📄 许可证

本项目采用Apache License 2.0许可证。

## 📞 联系我们

如有问题或建议，请提交Issue或联系项目维护者。

---

**注意**：这是一个示例项目，生产环境使用前请进行充分的测试和安全评估。
