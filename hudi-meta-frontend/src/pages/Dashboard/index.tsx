import React from 'react';
import { Card, Row, Col, Statistic, Progress, Table, Tag, Space } from 'antd';
import {
  DatabaseOutlined,
  FileTextOutlined,
  SyncOutlined,
  WarningOutlined,
  CheckCircleOutlined,
  ClockCircleOutlined,
} from '@ant-design/icons';

/**
 * 仪表板组件
 * 显示系统整体状态和统计信息
 */
const Dashboard: React.FC = () => {
  // 模拟数据
  const stats = {
    totalTables: 156,
    activeTables: 142,
    syncingTables: 8,
    errorTables: 6,
    totalRecords: 12453678,
    todaySync: 1234,
  };

  // 最近同步任务数据
  const recentTasks = [
    {
      key: '1',
      tableName: 'user_info',
      status: 'success',
      records: 1234,
      duration: '2分钟',
      timestamp: '2024-01-15 10:30:00',
    },
    {
      key: '2',
      tableName: 'order_details',
      status: 'running',
      records: 856,
      duration: '1分钟',
      timestamp: '2024-01-15 10:28:00',
    },
    {
      key: '3',
      tableName: 'product_catalog',
      status: 'error',
      records: 0,
      duration: '30秒',
      timestamp: '2024-01-15 10:25:00',
    },
    {
      key: '4',
      tableName: 'inventory_log',
      status: 'success',
      records: 2340,
      duration: '3分钟',
      timestamp: '2024-01-15 10:22:00',
    },
  ];

  // 任务状态渲染
  const renderStatus = (status: string) => {
    const statusMap = {
      success: { color: 'green', icon: <CheckCircleOutlined />, text: '成功' },
      running: { color: 'blue', icon: <SyncOutlined spin />, text: '运行中' },
      error: { color: 'red', icon: <WarningOutlined />, text: '失败' },
    };
    const config = statusMap[status as keyof typeof statusMap];
    return (
      <Tag color={config.color} icon={config.icon}>
        {config.text}
      </Tag>
    );
  };

  // 表格列定义
  const columns = [
    {
      title: '表名',
      dataIndex: 'tableName',
      key: 'tableName',
      render: (text: string) => <strong>{text}</strong>,
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      render: renderStatus,
    },
    {
      title: '记录数',
      dataIndex: 'records',
      key: 'records',
      render: (value: number) => value.toLocaleString(),
    },
    {
      title: '耗时',
      dataIndex: 'duration',
      key: 'duration',
    },
    {
      title: '时间',
      dataIndex: 'timestamp',
      key: 'timestamp',
      render: (text: string) => (
        <span style={{ color: '#666' }}>
          <ClockCircleOutlined style={{ marginRight: 4 }} />
          {text}
        </span>
      ),
    },
  ];

  return (
    <div>
      <Space direction="vertical" size="large" style={{ width: '100%' }}>
        {/* 统计卡片 */}
        <Row gutter={16}>
          <Col span={6}>
            <Card>
              <Statistic
                title="总表数"
                value={stats.totalTables}
                prefix={<DatabaseOutlined />}
                valueStyle={{ color: '#3f8600' }}
              />
            </Card>
          </Col>
          <Col span={6}>
            <Card>
              <Statistic
                title="活跃表数"
                value={stats.activeTables}
                prefix={<CheckCircleOutlined />}
                valueStyle={{ color: '#1890ff' }}
              />
            </Card>
          </Col>
          <Col span={6}>
            <Card>
              <Statistic
                title="同步中"
                value={stats.syncingTables}
                prefix={<SyncOutlined spin />}
                valueStyle={{ color: '#faad14' }}
              />
            </Card>
          </Col>
          <Col span={6}>
            <Card>
              <Statistic
                title="异常表数"
                value={stats.errorTables}
                prefix={<WarningOutlined />}
                valueStyle={{ color: '#cf1322' }}
              />
            </Card>
          </Col>
        </Row>

        {/* 详细统计 */}
        <Row gutter={16}>
          <Col span={12}>
            <Card title="数据统计" extra={<FileTextOutlined />}>
              <Space direction="vertical" style={{ width: '100%' }}>
                <Statistic
                  title="总记录数"
                  value={stats.totalRecords}
                  formatter={(value) => `${value}`.replace(/\B(?=(\d{3})+(?!\d))/g, ',')}
                />
                <Statistic
                  title="今日同步"
                  value={stats.todaySync}
                  suffix="条"
                  valueStyle={{ color: '#1890ff' }}
                />
              </Space>
            </Card>
          </Col>
          <Col span={12}>
            <Card title="系统健康度">
              <Space direction="vertical" style={{ width: '100%' }}>
                <div>
                  <div style={{ marginBottom: 8 }}>表活跃率</div>
                  <Progress
                    percent={Math.round((stats.activeTables / stats.totalTables) * 100)}
                    status="active"
                    strokeColor={{
                      from: '#108ee9',
                      to: '#87d068',
                    }}
                  />
                </div>
                <div>
                  <div style={{ marginBottom: 8 }}>同步成功率</div>
                  <Progress
                    percent={94}
                    status="active"
                    strokeColor={{
                      '0%': '#108ee9',
                      '100%': '#87d068',
                    }}
                  />
                </div>
              </Space>
            </Card>
          </Col>
        </Row>

        {/* 最近任务 */}
        <Card title="最近同步任务" extra={<SyncOutlined />}>
          <Table
            columns={columns}
            dataSource={recentTasks}
            pagination={false}
            size="small"
          />
        </Card>
      </Space>
    </div>
  );
};

export default Dashboard; 