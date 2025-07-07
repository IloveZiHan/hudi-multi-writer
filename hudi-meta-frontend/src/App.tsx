import React from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import { ConfigProvider, Layout, Menu, theme } from 'antd';
import {
  DatabaseOutlined,
  DashboardOutlined,
  SettingOutlined,
  UserOutlined,
} from '@ant-design/icons';
import zhCN from 'antd/locale/zh_CN';
import dayjs from 'dayjs';
import 'dayjs/locale/zh-cn';
import TableManagement from '@pages/TableManagement';
import Dashboard from '@pages/Dashboard';
import Settings from '@pages/Settings';
import './App.css';

const { Header, Content, Sider } = Layout;

// 设置dayjs中文
dayjs.locale('zh-cn');

/**
 * 主应用组件
 */
const App: React.FC = () => {
  const {
    token: { colorBgContainer },
  } = theme.useToken();

  // 菜单项配置
  const menuItems = [
    {
      key: 'dashboard',
      icon: <DashboardOutlined />,
      label: '仪表板',
      path: '/dashboard',
    },
    {
      key: 'tables',
      icon: <DatabaseOutlined />,
      label: '表管理',
      path: '/tables',
    },
    {
      key: 'settings',
      icon: <SettingOutlined />,
      label: '系统设置',
      path: '/settings',
    },
  ];

  return (
    <ConfigProvider
      locale={zhCN}
      theme={{
        token: {
          colorPrimary: '#1890ff',
          borderRadius: 6,
        },
        components: {
          Layout: {
            siderBg: '#001529',
            triggerBg: '#002140',
          },
        },
      }}
    >
      <Router>
        <Layout style={{ minHeight: '100vh' }}>
          {/* 侧边栏 */}
          <Sider
            collapsible
            theme="dark"
            breakpoint="lg"
            collapsedWidth="0"
            style={{
              overflow: 'auto',
              height: '100vh',
              position: 'fixed',
              left: 0,
              top: 0,
              bottom: 0,
            }}
          >
            <div className="logo">
              <DatabaseOutlined style={{ fontSize: 24, color: '#1890ff' }} />
              <span>Hudi Meta</span>
            </div>
            <Menu
              theme="dark"
              mode="inline"
              defaultSelectedKeys={['tables']}
              items={menuItems.map(item => ({
                key: item.key,
                icon: item.icon,
                label: (
                  <span
                    onClick={() => {
                      window.location.hash = item.path;
                    }}
                  >
                    {item.label}
                  </span>
                ),
              }))}
            />
          </Sider>

          {/* 主内容区 */}
          <Layout style={{ marginLeft: 200 }}>
            {/* 头部 */}
            <Header
              style={{
                padding: '0 24px',
                background: colorBgContainer,
                borderBottom: '1px solid #f0f0f0',
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
              }}
            >
              <div>
                <h2 style={{ margin: 0 }}>Hudi Meta Table 管理系统</h2>
              </div>
              <div style={{ display: 'flex', alignItems: 'center' }}>
                <UserOutlined style={{ fontSize: 16, marginRight: 8 }} />
                <span>管理员</span>
              </div>
            </Header>

            {/* 内容区 */}
            <Content
              style={{
                margin: '24px 16px',
                padding: 24,
                background: colorBgContainer,
                minHeight: 280,
                borderRadius: 8,
              }}
            >
              <Routes>
                <Route path="/" element={<Navigate to="/tables" replace />} />
                <Route path="/dashboard" element={<Dashboard />} />
                <Route path="/tables" element={<TableManagement />} />
                <Route path="/settings" element={<Settings />} />
                <Route path="*" element={<Navigate to="/tables" replace />} />
              </Routes>
            </Content>
          </Layout>
        </Layout>
      </Router>
    </ConfigProvider>
  );
};

export default App; 