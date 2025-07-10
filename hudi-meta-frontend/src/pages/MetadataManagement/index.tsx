import React, { useState, useEffect } from 'react';
import {
  Table,
  Card,
  Space,
  Tag,
  Button,
  Tooltip,
  Row,
  Col,
  Statistic,
  Alert,
  Typography,
  Badge,
  Divider,
  message,
} from 'antd';
import {
  DatabaseOutlined,
  ReloadOutlined,
  EyeOutlined,
  InfoCircleOutlined,
  CheckCircleOutlined,
  CloseCircleOutlined,
  TableOutlined,
  BarChartOutlined,
  FileTextOutlined,
  ImportOutlined,
  PlusOutlined,
} from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import { useRequest } from 'ahooks';
import dayjs from 'dayjs';
import tableApiService from '@services/tableApi';
import ImportFromBusinessTableModal, { ImportTableData } from './ImportFromBusinessTableModal';
import CreateTableModal from '../TableManagement/CreateTableModal';
import './index.less';

const { Title, Text } = Typography;

// 系统表信息接口
interface SystemTableInfo {
  tableName: string;
  tableType: string;
  description: string;
  status: 'EXISTS' | 'NOT_EXISTS';
  recordCount: number;
  size: string;
  lastModified: string;
  schema: any;
  location: string;
}

/**
 * 元数据管理页面
 * 展示Hudi系统表信息
 */
const MetadataManagement: React.FC = () => {
  const [loading, setLoading] = useState(false);
  const [selectedTable, setSelectedTable] = useState<SystemTableInfo | null>(null);
  const [importModalVisible, setImportModalVisible] = useState(false);
  const [createTableModalVisible, setCreateTableModalVisible] = useState(false);
  const [importedTableData, setImportedTableData] = useState<ImportTableData | null>(null);

  // 获取系统表列表
  const { data: systemTables, loading: tablesLoading, run: fetchSystemTables } = useRequest(
    () => tableApiService.getSystemTables(),
    {
      onError: (error) => {
        message.error(`获取系统表失败: ${error.message}`);
      },
    }
  );

  // 获取系统表统计信息
  const { data: statsData, run: fetchStats } = useRequest(
    () => tableApiService.getSystemTableStats(),
    {
      onError: (error) => {
        message.error(`获取统计信息失败: ${error.message}`);
      },
    }
  );

  // 系统表列定义
  const columns: ColumnsType<SystemTableInfo> = [
    {
      title: '表名',
      dataIndex: 'tableName',
      key: 'tableName',
      width: 200,
      fixed: 'left',
      render: (text: string, record: SystemTableInfo) => (
        <Space>
          <TableOutlined style={{ color: '#1890ff' }} />
          <Text strong>{text}</Text>
        </Space>
      ),
    },
    {
      title: '类型',
      dataIndex: 'tableType',
      key: 'tableType',
      width: 120,
      render: (type: string) => {
        const typeColors = {
          'METADATA': '#722ed1',
          'SYSTEM': '#52c41a',
          'TIMELINE': '#13c2c2',
          'COMPACTION': '#fa8c16',
          'CLEANER': '#eb2f96',
        };
        return (
          <Tag color={typeColors[type as keyof typeof typeColors] || '#666'}>
            {type}
          </Tag>
        );
      },
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      width: 100,
      render: (status: string) => (
        <Badge
          status={status === 'EXISTS' ? 'success' : 'error'}
          text={status === 'EXISTS' ? '存在' : '不存在'}
        />
      ),
    },
    {
      title: '记录数',
      dataIndex: 'recordCount',
      key: 'recordCount',
      width: 120,
      render: (count: number) => (
        <span style={{ color: count > 0 ? '#52c41a' : '#999' }}>
          {count?.toLocaleString() || 0}
        </span>
      ),
    },
    {
      title: '大小',
      dataIndex: 'size',
      key: 'size',
      width: 100,
      render: (size: string) => (
        <span style={{ color: '#666' }}>{size || '-'}</span>
      ),
    },
    {
      title: '最后修改',
      dataIndex: 'lastModified',
      key: 'lastModified',
      width: 160,
      render: (time: string) => (
        <span style={{ color: '#666' }}>
          {time ? dayjs(time).format('YYYY-MM-DD HH:mm:ss') : '-'}
        </span>
      ),
    },
    {
      title: '位置',
      dataIndex: 'location',
      key: 'location',
      width: 300,
      render: (location: string) => (
        <Tooltip title={location}>
          <Text code style={{ fontSize: '11px' }}>
            {location ? location.substring(0, 50) + '...' : '-'}
          </Text>
        </Tooltip>
      ),
    },
    {
      title: '描述',
      dataIndex: 'description',
      key: 'description',
      width: 250,
      render: (description: string) => (
        <Text style={{ color: '#666' }}>{description}</Text>
      ),
    },
    {
      title: '操作',
      key: 'action',
      fixed: 'right',
      width: 120,
      render: (_, record) => (
        <Space size="small">
          <Tooltip title="查看详情">
            <Button
              type="text"
              icon={<EyeOutlined />}
              onClick={() => handleViewDetail(record)}
              disabled={record.status !== 'EXISTS'}
            />
          </Tooltip>
          <Tooltip title="刷新">
            <Button
              type="text"
              icon={<ReloadOutlined />}
              onClick={() => handleRefreshTable(record)}
              size="small"
            />
          </Tooltip>
        </Space>
      ),
    },
  ];

  // 处理查看详情
  const handleViewDetail = (record: SystemTableInfo) => {
    setSelectedTable(record);
    message.info(`查看表 ${record.tableName} 的详情`);
  };

  // 处理刷新单个表
  const handleRefreshTable = async (record: SystemTableInfo) => {
    setLoading(true);
    try {
      await tableApiService.refreshSystemTable(record.tableName);
      message.success(`刷新表 ${record.tableName} 成功`);
      fetchSystemTables();
    } catch (error) {
      message.error(`刷新表失败: ${error}`);
    } finally {
      setLoading(false);
    }
  };

  // 处理全部刷新
  const handleRefreshAll = async () => {
    setLoading(true);
    try {
      await Promise.all([fetchSystemTables(), fetchStats()]);
      message.success('刷新成功');
    } catch (error) {
      message.error(`刷新失败: ${error}`);
    } finally {
      setLoading(false);
    }
  };

  // 处理从业务表导入
  const handleImportFromBusinessTable = () => {
    setImportModalVisible(true);
  };

  // 处理导入完成
  const handleImportComplete = (importData: ImportTableData) => {
    setImportedTableData(importData);
    setImportModalVisible(false);
    setCreateTableModalVisible(true);
  };

  // 处理创建表成功
  const handleCreateTableSuccess = () => {
    setCreateTableModalVisible(false);
    setImportedTableData(null);
    message.success('表创建成功！');
    // 刷新系统表列表
    fetchSystemTables();
  };

  // 处理创建表取消
  const handleCreateTableCancel = () => {
    setCreateTableModalVisible(false);
    setImportedTableData(null);
  };

  // 获取状态统计
  const getStatusStats = () => {
    if (!systemTables) return { total: 0, exists: 0, notExists: 0 };
    
    const total = systemTables.length;
    const exists = systemTables.filter(t => t.status === 'EXISTS').length;
    const notExists = total - exists;
    
    return { total, exists, notExists };
  };

  const statusStats = getStatusStats();

  return (
    <div className="metadata-management">
      {/* 页面标题 */}
      <div className="page-header">
        <Title level={2}>
          <DatabaseOutlined />
          元数据管理
        </Title>
        <Text type="secondary">
          管理和监控Hudi系统元数据表，包括表结构、状态和统计信息
        </Text>
      </div>

      {/* 统计卡片 */}
      <Row gutter={16} className="stats-cards">
        <Col span={6}>
          <Card>
            <Statistic
              title="总表数"
              value={statusStats.total}
              prefix={<TableOutlined />}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="存在表"
              value={statusStats.exists}
              valueStyle={{ color: '#52c41a' }}
              prefix={<CheckCircleOutlined />}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="缺失表"
              value={statusStats.notExists}
              valueStyle={{ color: '#f5222d' }}
              prefix={<CloseCircleOutlined />}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="总记录数"
              value={systemTables?.reduce((sum, t) => sum + (t.recordCount || 0), 0) || 0}
              prefix={<BarChartOutlined />}
            />
          </Card>
        </Col>
      </Row>

      {/* 提示信息 */}
      <Alert
        message="系统表说明"
        description={
          <div>
            <p style={{ marginBottom: 8 }}>
              以下为Hudi系统的元数据表，这些表存储了表结构、分区信息、提交历史等重要数据：
            </p>
            <ul style={{ marginBottom: 0, paddingLeft: 20 }}>
              <li><strong>METADATA表</strong>：存储表的元数据信息</li>
              <li><strong>SYSTEM表</strong>：存储系统级别的配置和状态</li>
              <li><strong>TIMELINE表</strong>：记录表的时间线和版本信息</li>
              <li><strong>COMPACTION表</strong>：管理压缩操作的相关信息</li>
              <li><strong>CLEANER表</strong>：管理清理操作的相关信息</li>
            </ul>
          </div>
        }
        type="info"
        showIcon
        style={{ marginBottom: 24 }}
      />

      {/* 系统表列表 */}
      <Card
        title={
          <Space>
            <FileTextOutlined />
            系统表列表
          </Space>
        }
        extra={
          <Space>
            <Button
              icon={<ImportOutlined />}
              onClick={handleImportFromBusinessTable}
              type="primary"
              ghost
            >
              从业务表导入
            </Button>
            <Button
              icon={<ReloadOutlined />}
              onClick={handleRefreshAll}
              loading={loading}
            >
              刷新
            </Button>
          </Space>
        }
      >
        <Table
          columns={columns}
          dataSource={systemTables || []}
          rowKey="tableName"
          loading={tablesLoading}
          scroll={{ x: 1400 }}
          pagination={false}
          size="middle"
          bordered
        />
      </Card>

      {/* 从业务表导入对话框 */}
      <ImportFromBusinessTableModal
        visible={importModalVisible}
        onCancel={() => setImportModalVisible(false)}
        onImport={handleImportComplete}
      />

      {/* 创建表对话框 */}
      {importedTableData && (
        <CreateTableModal
          visible={createTableModalVisible}
          onCancel={handleCreateTableCancel}
          onSuccess={handleCreateTableSuccess}
          importData={importedTableData}
        />
      )}
    </div>
  );
};

export default MetadataManagement; 