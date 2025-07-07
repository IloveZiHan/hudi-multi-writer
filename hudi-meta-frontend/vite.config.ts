import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import { resolve } from 'path'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  
  // 路径别名配置
  resolve: {
    alias: {
      '@': resolve(__dirname, 'src'),
      '@components': resolve(__dirname, 'src/components'),
      '@pages': resolve(__dirname, 'src/pages'),
      '@services': resolve(__dirname, 'src/services'),
      '@utils': resolve(__dirname, 'src/utils'),
      '@types': resolve(__dirname, 'src/types'),
      '@hooks': resolve(__dirname, 'src/hooks'),
      '@styles': resolve(__dirname, 'src/styles'),
    },
  },
  
  // 开发服务器配置
  server: {
    port: 3000,
    host: true,
    open: true,
    proxy: {
      // 代理API请求到后端服务器
      '/api': {
        target: 'http://localhost:8080',
        changeOrigin: true,
        secure: false,
      },
    },
  },
  
  // 构建配置
  build: {
    outDir: 'dist',
    sourcemap: false,
    minify: 'terser',
    terserOptions: {
      compress: {
        drop_console: true,
        drop_debugger: true,
      },
    },
    rollupOptions: {
      output: {
        chunkFileNames: 'js/[name]-[hash].js',
        entryFileNames: 'js/[name]-[hash].js',
        assetFileNames: '[ext]/[name]-[hash].[ext]',
      },
    },
  },
  
  // CSS配置
  css: {
    preprocessorOptions: {
      less: {
        javascriptEnabled: true,
        modifyVars: {
          // Ant Design主题定制
          '@primary-color': '#1890ff',
          '@success-color': '#52c41a',
          '@warning-color': '#faad14',
          '@error-color': '#f5222d',
          '@font-size-base': '14px',
          '@border-radius-base': '6px',
        },
      },
    },
  },
  
  // 依赖优化
  optimizeDeps: {
    include: ['react', 'react-dom', 'antd', 'axios', 'dayjs'],
  },
  
  // 测试配置
  test: {
    globals: true,
    environment: 'jsdom',
    setupFiles: ['./src/test/setup.ts'],
  },
}) 