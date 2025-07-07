# Hudi Meta Table 管理系统 - 前端开发文档

## 项目简介

Hudi Meta Table 管理系统是一个用于管理 Apache Hudi 表元数据的 Web 应用程序。该系统提供了直观的用户界面，用于创建、编辑、查看和管理 Hudi 表的元数据信息。

## 技术栈

- **前端框架**: React 18.2.0
- **开发语言**: TypeScript 5.2.2
- **构建工具**: Vite 5.0.0
- **UI 组件库**: Ant Design 5.12.1
- **路由管理**: React Router DOM 6.20.1
- **HTTP 客户端**: Axios 1.6.2
- **状态管理**: React Hooks + ahooks 3.7.8
- **图表库**: Ant Design Charts 2.0.3
- **时间处理**: dayjs 1.11.10
- **工具函数**: Lodash 4.17.21

## 环境要求

- **Node.js**: >= 16.0.0
- **npm**: >= 7.0.0
- **操作系统**: Windows、macOS 或 Linux

## 快速开始

### 1. 克隆项目

```bash
# 如果您还没有克隆整个项目
git clone <repository-url>
cd big_data/hudi-multi-writer/hudi-meta-frontend
```

### 2. 安装依赖

```bash
npm install
```

### 3. 启动开发服务器

```bash
npm run dev
```

开发服务器将在 `http://localhost:3000` 启动，并自动打开浏览器。

## 完整的开发流程

### 步骤 1: 环境检查

确保您的开发环境满足要求：

```bash
# 检查 Node.js 版本
node --version

# 检查 npm 版本
npm --version
```

### 步骤 2: 项目配置

项目已经配置好了所有必要的设置，包括：

- **TypeScript 配置**: `tsconfig.json` 和 `tsconfig.node.json`
- **ESLint 配置**: `.eslintrc.cjs`
- **Prettier 配置**: `.prettierrc`
- **Vite 配置**: `vite.config.ts`

### 步骤 3: 后端服务

前端应用需要连接到后端 API 服务。根据 `vite.config.ts` 配置，前端会将 API 请求代理到 `http://localhost:8080`。

确保后端服务正在运行：

```bash
# 后端服务应该在 8080 端口运行
# 具体启动方式请参考后端项目文档
```

### 步骤 4: 开发命令

```bash
# 启动开发服务器
npm run dev

# 构建生产版本
npm run build

# 预览构建结果
npm run preview

# 代码检查
npm run lint

# 自动修复代码问题
npm run lint:fix

# 代码格式化
npm run format

# 类型检查
npm run type-check

# 运行测试
npm run test
```

## 项目结构

```
hudi-meta-frontend/
├── public/                 # 静态资源
├── src/                    # 源代码
│   ├── components/         # 通用组件
│   │   ├── Dashboard/      # 仪表板页面
│   │   ├── TableManagement/# 表管理页面
│   │   └── Settings/       # 设置页面
│   ├── services/           # API 服务
│   │   ├── http.ts         # HTTP 客户端
│   │   └── tableApi.ts     # 表管理 API
│   ├── types/              # TypeScript 类型定义
│   │   └── api.ts          # API 相关类型
│   ├── hooks/              # 自定义 Hooks
│   ├── utils/              # 工具函数
│   ├── styles/             # 样式文件
│   ├── App.tsx             # 主应用组件
│   ├── main.tsx            # 应用入口
│   └── index.css           # 全局样式
├── .eslintrc.cjs           # ESLint 配置
├── .prettierrc             # Prettier 配置
├── tsconfig.json           # TypeScript 配置
├── tsconfig.node.json      # Node.js TypeScript 配置
├── vite.config.ts          # Vite 配置
├── package.json            # 项目依赖和脚本
└── index.html              # HTML 模板
```

## 功能特性

- **表管理**: 创建、编辑、删除和查看 Hudi 表元数据
- **批量操作**: 支持批量删除、上线、下线表
- **高级搜索**: 根据多种条件搜索表
- **数据统计**: 显示表状态统计信息
- **数据导出**: 支持导出表数据为 Excel 文件
- **响应式设计**: 适配不同屏幕尺寸

## 开发建议

### 1. 代码规范

项目已配置了 ESLint 和 Prettier，请确保：

- 提交代码前运行 `npm run lint:fix`
- 使用 `npm run format` 格式化代码
- 遵循 TypeScript 严格模式

### 2. 组件开发

- 使用函数组件和 Hooks
- 为组件添加 TypeScript 类型
- 遵循 React 最佳实践

### 3. API 集成

- 所有 API 调用都通过 `services/tableApi.ts` 进行
- 使用 `ahooks` 的 `useRequest` 进行数据获取
- 统一的错误处理和加载状态管理

### 4. 样式管理

- 优先使用 Ant Design 组件
- 自定义样式使用 Less 或 CSS Modules
- 保持样式的一致性和可维护性

## 常见问题

### 1. 安装依赖失败

```bash
# 清除 npm 缓存
npm cache clean --force

# 删除 node_modules 和重新安装
rm -rf node_modules package-lock.json
npm install
```

### 2. 端口冲突

如果 3000 端口被占用，可以修改 `vite.config.ts` 中的端口配置：

```typescript
server: {
  port: 3001, // 修改为其他端口
  // ...
}
```

### 3. API 请求失败

确保后端服务正在运行，并检查 `vite.config.ts` 中的代理配置：

```typescript
proxy: {
  '/api': {
    target: 'http://localhost:8080', // 确保后端服务地址正确
    changeOrigin: true,
    secure: false,
  },
}
```

### 4. 构建失败

检查 TypeScript 类型错误：

```bash
npm run type-check
```

### 5. 热重载不工作

检查文件保存权限和防火墙设置，或重启开发服务器。

## 生产部署

### 1. 构建生产版本

```bash
npm run build
```

### 2. 部署到服务器

构建完成后，`dist` 目录包含所有生产文件。将这些文件部署到您的 Web 服务器即可。

### 3. 环境变量配置

根据不同环境，您可能需要配置不同的 API 端点和其他环境变量。

## 支持与反馈

如果您在开发过程中遇到问题，请：

1. 检查本文档的常见问题部分
2. 查看项目的 issue 列表
3. 联系项目维护者

---

**注意**: 这个项目是 Hudi Multi-Writer 系统的前端部分，需要配合后端服务一起使用。确保后端服务正常运行后再启动前端应用。 