# 快速开始指南

## 🚀 5分钟启动项目

### 前置条件
- Node.js >= 16.0.0
- npm >= 7.0.0

### 步骤 1: 进入项目目录
```bash
cd hudi-multi-writer/hudi-meta-frontend
```

### 步骤 2: 安装依赖
```bash
npm install
```

### 步骤 3: 启动开发服务器
```bash
npm run dev
```

✅ 浏览器会自动打开 `http://localhost:3000`

## 📝 重要提示

### 后端服务
- 前端需要连接到后端 API 服务（端口 8080）
- 确保后端服务正在运行，否则会有 API 请求错误

### 开发命令
```bash
# 开发服务器
npm run dev

# 构建生产版本
npm run build

# 代码检查
npm run lint

# 代码格式化
npm run format
```

### 项目特点
- 🎨 使用 Ant Design 组件库
- 📱 响应式设计，支持移动端
- 🔧 TypeScript 类型安全
- 📦 Vite 快速构建
- 🎯 ESLint + Prettier 代码规范

### 常见问题
1. **端口冲突**: 修改 `vite.config.ts` 中的端口号
2. **API 请求失败**: 确保后端服务运行在 8080 端口
3. **依赖安装失败**: 清除缓存 `npm cache clean --force`

---

详细文档请参考 [README.md](./README.md) 