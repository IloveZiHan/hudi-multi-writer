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
  Modal,
  Form,
  Input,
  Select,
  Popconfirm,
  DatePicker,
} from 'antd';
import {
  DatabaseOutlined,
  ReloadOutlined,
  EyeOutlined,
  InfoCircleOutlined,
  CheckCircleOutlined,
  CloseCircleOutlined,
  TableOutlined,
  PlusOutlined,
  EditOutlined,
  DeleteOutlined,
  SearchOutlined,
  ExperimentOutlined,
  CloudServerOutlined,
} from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import { useRequest } from 'ahooks';
import dayjs from 'dayjs';
import dbServerApiService from '@services/dbServerApi';
import './index.less';

const { Title, Text } = Typography;
const { Option } = Select;
const { RangePicker } = DatePicker;

// 数据库服务器信息接口
interface DbServerInfo {
  id: number;
  name: string;
  host: string;
  sourceType: string;
  jdbcUrl: string;
  username: string;
  password: string;
  alias: string;
  description: string;
  createUser: string;
  createTime: string;
  updateUser: string;
  updateTime: string;
}

// 搜索条件接口
interface SearchCriteria {
  keyword?: string;
  sourceType?: string;
  host?: string;
  createUser?: string;
  createTimeStart?: string;
  createTimeEnd?: string;
}

// 创建表单接口
interface CreateDbServerForm {
  name: string;
  host: string;
  sourceType: string;
  jdbcUrl: string;
  username: string;
  password: string;
  alias?: string;
  description?: string;
  createUser: string;
}

// 更新表单接口
interface UpdateDbServerForm {
  name?: string;
  host?: string;
  sourceType?: string;
  jdbcUrl?: string;
  username?: string;
  password?: string;
  alias?: string;
  description?: string;
  updateUser?: string;
}

/**
 * 数据库服务器管理页面
 */
const DbServerManagement: React.FC = () => {
  const [loading, setLoading] = useState(false);
  const [searchCriteria, setSearchCriteria] = useState<SearchCriteria>({});
  const [selectedServer, setSelectedServer] = useState<DbServerInfo | null>(null);
  const [createModalVisible, setCreateModalVisible] = useState(false);
  const [editModalVisible, setEditModalVisible] = useState(false);
  const [detailModalVisible, setDetailModalVisible] = useState(false);
  const [selectedRowKeys, setSelectedRowKeys] = useState<number[]>([]);
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(10);
  const [createForm] = Form.useForm();
  const [editForm] = Form.useForm();
  const [searchForm] = Form.useForm();

  // 获取数据库服务器列表
  const { data: serverData, loading: serversLoading, run: fetchServers } = useRequest(
    () => dbServerApiService.getAllDbServers(currentPage - 1, pageSize),
    {
      refreshDeps: [currentPage, pageSize],
      onError: (error) => {
        console.error('获取数据库服务器列表失败:', error);
        message.error(`获取数据库服务器列表失败: ${error?.message || '未知错误'}`);
      },
      defaultData: { data: [], total: 0, page: 0, size: 10, hasNext: false, hasPrevious: false }
    }
  );

  // 获取支持的数据库类型
  const { data: supportedTypes } = useRequest(
    () => dbServerApiService.getSupportedSourceTypes(),
    {
      onError: (error) => {
        console.error('获取支持的数据库类型失败:', error);
        message.error(`获取支持的数据库类型失败: ${error?.message || '未知错误'}`);
      },
      defaultData: ['mysql', 'oracle', 'postgresql', 'sqlserver', 'tdsql', 'clickhouse', 'tidb']
    }
  );

  // 数据库服务器列定义
  const columns: ColumnsType<DbServerInfo> = [
    {
      title: '服务器名称',
      dataIndex: 'name',
      key: 'name',
      width: 200,
      fixed: 'left',
      render: (text: string, record: DbServerInfo) => (
        <Space>
          <CloudServerOutlined style={{ color: '#1890ff' }} />
          <Button
            type="link"
            onClick={() => handleViewDetail(record)}
            style={{ padding: 0 }}
          >
            {text}
          </Button>
        </Space>
      ),
    },
    {
      title: '数据库类型',
      dataIndex: 'sourceType',
      key: 'sourceType',
      width: 120,
      render: (type: string) => {
        const typeColors = {
          'mysql': '#52c41a',
          'oracle': '#1890ff',
          'postgresql': '#722ed1',
          'sqlserver': '#13c2c2',
          'tdsql': '#fa8c16',
          'clickhouse': '#eb2f96',
          'tidb': '#faad14',
        };
        return (
          <Tag color={typeColors[type as keyof typeof typeColors] || '#666'}>
            {type.toUpperCase()}
          </Tag>
        );
      },
    },
    {
      title: '服务器地址',
      dataIndex: 'host',
      key: 'host',
      width: 150,
      render: (host: string) => (
        <Text code style={{ fontSize: '12px' }}>{host}</Text>
      ),
    },
    {
      title: '用户名',
      dataIndex: 'username',
      key: 'username',
      width: 120,
    },
    {
      title: '别名',
      dataIndex: 'alias',
      key: 'alias',
      width: 100,
      render: (alias: string) => (
        <Text style={{ color: '#666' }}>{alias || '-'}</Text>
      ),
    },
    {
      title: '描述',
      dataIndex: 'description',
      key: 'description',
      width: 200,
      render: (description: string) => (
        <Tooltip title={description}>
          <Text ellipsis style={{ color: '#666' }}>
            {description || '-'}
          </Text>
        </Tooltip>
      ),
    },
    {
      title: '创建人',
      dataIndex: 'createUser',
      key: 'createUser',
      width: 100,
    },
    {
      title: '创建时间',
      dataIndex: 'createTime',
      key: 'createTime',
      width: 160,
      render: (time: string) => (
        <span style={{ color: '#666' }}>
          {time ? dayjs(time).format('YYYY-MM-DD HH:mm:ss') : '-'}
        </span>
      ),
    },
    {
      title: '操作',
      key: 'action',
      fixed: 'right',
      width: 200,
      render: (_, record) => (
        <Space size="small">
          <Tooltip title="查看详情">
            <Button
              type="text"
              icon={<EyeOutlined />}
              onClick={() => handleViewDetail(record)}
              size="small"
            />
          </Tooltip>
          <Tooltip title="编辑">
            <Button
              type="text"
              icon={<EditOutlined />}
              onClick={() => handleEdit(record)}
              size="small"
            />
          </Tooltip>
          <Tooltip title="测试连接">
            <Button
              type="text"
              icon={<ExperimentOutlined />}
              onClick={() => handleTestConnection(record)}
              size="small"
            />
          </Tooltip>
          <Tooltip title="删除">
            <Popconfirm
              title="确认删除此数据库服务器?"
              onConfirm={() => handleDelete(record)}
              okText="确认"
              cancelText="取消"
            >
              <Button
                type="text"
                icon={<DeleteOutlined />}
                danger
                size="small"
              />
            </Popconfirm>
          </Tooltip>
        </Space>
      ),
    },
  ];

  // 处理查看详情
  const handleViewDetail = (record: DbServerInfo) => {
    setSelectedServer(record);
    setDetailModalVisible(true);
  };

  // 处理编辑
  const handleEdit = (record: DbServerInfo) => {
    setSelectedServer(record);
    editForm.setFieldsValue({
      name: record.name,
      host: record.host,
      sourceType: record.sourceType,
      jdbcUrl: record.jdbcUrl,
      username: record.username,
      password: record.password,
      alias: record.alias,
      description: record.description,
      updateUser: 'admin', // 默认更新人
    });
    setEditModalVisible(true);
  };

  // 处理删除
  const handleDelete = async (record: DbServerInfo) => {
    try {
      await dbServerApiService.deleteDbServer(record.id);
      message.success(`删除数据库服务器 ${record.name} 成功`);
      fetchServers();
    } catch (error) {
      console.error('删除数据库服务器失败:', error);
      message.error(`删除数据库服务器失败: ${error?.message || '未知错误'}`);
    }
  };

  // 处理测试连接
  const handleTestConnection = async (record: DbServerInfo) => {
    setLoading(true);
    try {
      const isConnected = await dbServerApiService.testDbConnection(record.id);
      if (isConnected) {
        message.success(`数据库服务器 ${record.name} 连接测试成功`);
      } else {
        message.error(`数据库服务器 ${record.name} 连接测试失败`);
      }
    } catch (error) {
      console.error('连接测试失败:', error);
      message.error(`连接测试失败: ${error?.message || '未知错误'}`);
    } finally {
      setLoading(false);
    }
  };

  // 处理创建
  const handleCreate = () => {
    createForm.setFieldsValue({
      createUser: 'admin', // 默认创建人
    });
    setCreateModalVisible(true);
  };

  // 处理创建确认
  const handleCreateSubmit = async () => {
    try {
      const values = await createForm.validateFields();
      await dbServerApiService.createDbServer(values);
      message.success('数据库服务器创建成功');
      setCreateModalVisible(false);
      createForm.resetFields();
      fetchServers();
    } catch (error) {
      console.error('创建数据库服务器失败:', error);
      message.error(`创建数据库服务器失败: ${error?.message || '未知错误'}`);
    }
  };

  // 处理编辑确认
  const handleEditSubmit = async () => {
    try {
      const values = await editForm.validateFields();
      if (!selectedServer) {
        message.error('未选择要编辑的服务器');
        return;
      }
      await dbServerApiService.updateDbServer(selectedServer.id, values);
      message.success('数据库服务器更新成功');
      setEditModalVisible(false);
      editForm.resetFields();
      fetchServers();
    } catch (error) {
      console.error('更新数据库服务器失败:', error);
      message.error(`更新数据库服务器失败: ${error?.message || '未知错误'}`);
    }
  };

  // 处理搜索
  const handleSearch = async () => {
    try {
      const values = await searchForm.getFieldsValue();
      const criteria: SearchCriteria = {
        keyword: values.keyword,
        sourceType: values.sourceType,
        host: values.host,
        createUser: values.createUser,
        createTimeStart: values.createTimeRange?.[0]?.format('YYYY-MM-DD HH:mm:ss'),
        createTimeEnd: values.createTimeRange?.[1]?.format('YYYY-MM-DD HH:mm:ss'),
      };
      setSearchCriteria(criteria);
      const result = await dbServerApiService.searchDbServers(criteria, 0, pageSize);
      // 这里应该更新数据
      message.success('搜索完成');
    } catch (error) {
      console.error('搜索失败:', error);
      message.error(`搜索失败: ${error?.message || '未知错误'}`);
    }
  };

  // 处理重置搜索
  const handleResetSearch = () => {
    searchForm.resetFields();
    setSearchCriteria({});
    fetchServers();
  };

  // 处理全部刷新
  const handleRefreshAll = async () => {
    setLoading(true);
    try {
      await fetchServers();
      message.success('刷新成功');
    } catch (error) {
      console.error('刷新失败:', error);
      message.error(`刷新失败: ${error?.message || '未知错误'}`);
    } finally {
      setLoading(false);
    }
  };

  // 处理批量删除
  const handleBatchDelete = async () => {
    if (selectedRowKeys.length === 0) {
      message.warning('请先选择要删除的数据库服务器');
      return;
    }

    try {
      const result = await dbServerApiService.batchDeleteDbServers(selectedRowKeys);
      message.success(`批量删除完成，成功: ${result.success}，失败: ${result.failed}`);
      setSelectedRowKeys([]);
      fetchServers();
    } catch (error) {
      console.error('批量删除失败:', error);
      message.error(`批量删除失败: ${error?.message || '未知错误'}`);
    }
  };

  // 获取统计信息
  const getStats = () => {
    if (!serverData) return { total: 0, mysql: 0, oracle: 0, others: 0 };
    
    const servers = serverData.data || [];
    const total = servers.length;
    const mysql = servers.filter(s => s.sourceType === 'mysql').length;
    const oracle = servers.filter(s => s.sourceType === 'oracle').length;
    const others = total - mysql - oracle;
    
    return { total, mysql, oracle, others };
  };

  const stats = getStats();

  // 行选择配置
  const rowSelection = {
    selectedRowKeys,
    onChange: (selectedRowKeys: React.Key[]) => {
      setSelectedRowKeys(selectedRowKeys as number[]);
    },
  };

  return (
    <div className="db-server-management">
      {/* 页面标题 */}
      <div className="page-header">
        <Title level={2}>
          <DatabaseOutlined />
          业务数据源管理
        </Title>
        <Text type="secondary">
          管理和维护业务系统的数据库连接信息，支持多种数据库类型
        </Text>
      </div>

      {/* 加载状态检查 */}
      {serversLoading && !serverData && (
        <div style={{ textAlign: 'center', padding: '50px' }}>
          <span>正在加载数据库服务器列表...</span>
        </div>
      )}

      {/* 统计卡片 */}
      <Row gutter={16} className="stats-cards">
        <Col span={6}>
          <Card>
            <Statistic
              title="总服务器数"
              value={stats.total}
              prefix={<CloudServerOutlined />}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="MySQL"
              value={stats.mysql}
              valueStyle={{ color: '#52c41a' }}
              prefix={<DatabaseOutlined />}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="Oracle"
              value={stats.oracle}
              valueStyle={{ color: '#1890ff' }}
              prefix={<DatabaseOutlined />}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="其他"
              value={stats.others}
              valueStyle={{ color: '#722ed1' }}
              prefix={<DatabaseOutlined />}
            />
          </Card>
        </Col>
      </Row>

      {/* 搜索表单 */}
      <Card title="搜索条件" style={{ marginBottom: 24 }}>
        <Form form={searchForm} layout="inline">
          <Form.Item name="keyword" label="关键词">
            <Input placeholder="名称/别名/描述" />
          </Form.Item>
          <Form.Item name="sourceType" label="数据库类型">
            <Select placeholder="请选择" allowClear style={{ width: 120 }}>
              {supportedTypes?.map(type => (
                <Option key={type} value={type}>{type.toUpperCase()}</Option>
              ))}
            </Select>
          </Form.Item>
          <Form.Item name="host" label="服务器地址">
            <Input placeholder="服务器地址" />
          </Form.Item>
          <Form.Item name="createUser" label="创建人">
            <Input placeholder="创建人" />
          </Form.Item>
          <Form.Item name="createTimeRange" label="创建时间">
            <RangePicker showTime />
          </Form.Item>
          <Form.Item>
            <Space>
              <Button type="primary" icon={<SearchOutlined />} onClick={handleSearch}>
                搜索
              </Button>
              <Button onClick={handleResetSearch}>重置</Button>
            </Space>
          </Form.Item>
        </Form>
      </Card>

      {/* 数据库服务器列表 */}
      <Card
        title={
          <Space>
            <DatabaseOutlined />
            数据库服务器列表
            <Badge count={stats.total} showZero color="#1890ff" />
          </Space>
        }
        extra={
          <Space>
            <Button type="primary" icon={<PlusOutlined />} onClick={handleCreate}>
              新建服务器
            </Button>
            <Button
              danger
              icon={<DeleteOutlined />}
              onClick={handleBatchDelete}
              disabled={selectedRowKeys.length === 0}
            >
              批量删除
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
          dataSource={serverData?.data || []}
          rowKey="id"
          loading={serversLoading}
          scroll={{ x: 1400 }}
          pagination={{
            current: currentPage,
            pageSize: pageSize,
            total: serverData?.total || 0,
            showSizeChanger: true,
            showQuickJumper: true,
            showTotal: (total, range) => `第 ${range[0]}-${range[1]} 项 共 ${total} 项`,
            onChange: (page, size) => {
              setCurrentPage(page);
              setPageSize(size || 10);
            },
          }}
          rowSelection={rowSelection}
          size="middle"
          bordered
        />
      </Card>

      {/* 创建数据库服务器模态框 */}
      <Modal
        title="创建数据库服务器"
        open={createModalVisible}
        onOk={handleCreateSubmit}
        onCancel={() => {
          setCreateModalVisible(false);
          createForm.resetFields();
        }}
        width={600}
      >
        <Form form={createForm} layout="vertical">
          <Form.Item
            name="name"
            label="服务器名称"
            rules={[{ required: true, message: '请输入服务器名称' }]}
          >
            <Input placeholder="请输入服务器名称" />
          </Form.Item>
          <Form.Item
            name="host"
            label="服务器地址"
            rules={[{ required: true, message: '请输入服务器地址' }]}
          >
            <Input placeholder="请输入服务器地址" />
          </Form.Item>
          <Form.Item
            name="sourceType"
            label="数据库类型"
            rules={[{ required: true, message: '请选择数据库类型' }]}
          >
            <Select placeholder="请选择数据库类型">
              {supportedTypes?.map(type => (
                <Option key={type} value={type}>{type.toUpperCase()}</Option>
              ))}
            </Select>
          </Form.Item>
          <Form.Item
            name="jdbcUrl"
            label="JDBC URL"
            rules={[{ required: true, message: '请输入JDBC URL' }]}
          >
            <Input placeholder="请输入JDBC URL" />
          </Form.Item>
          <Form.Item
            name="username"
            label="用户名"
            rules={[{ required: true, message: '请输入用户名' }]}
          >
            <Input placeholder="请输入用户名" />
          </Form.Item>
          <Form.Item
            name="password"
            label="密码"
            rules={[{ required: true, message: '请输入密码' }]}
          >
            <Input.Password placeholder="请输入密码" />
          </Form.Item>
          <Form.Item name="alias" label="别名">
            <Input placeholder="请输入别名" />
          </Form.Item>
          <Form.Item name="description" label="描述">
            <Input.TextArea placeholder="请输入描述" rows={3} />
          </Form.Item>
          <Form.Item name="createUser" label="创建人">
            <Input placeholder="创建人" />
          </Form.Item>
        </Form>
      </Modal>

      {/* 编辑数据库服务器模态框 */}
      <Modal
        title="编辑数据库服务器"
        open={editModalVisible}
        onOk={handleEditSubmit}
        onCancel={() => {
          setEditModalVisible(false);
          editForm.resetFields();
        }}
        width={600}
      >
        <Form form={editForm} layout="vertical">
          <Form.Item name="name" label="服务器名称">
            <Input placeholder="请输入服务器名称" />
          </Form.Item>
          <Form.Item name="host" label="服务器地址">
            <Input placeholder="请输入服务器地址" />
          </Form.Item>
          <Form.Item name="sourceType" label="数据库类型">
            <Select placeholder="请选择数据库类型">
              {supportedTypes?.map(type => (
                <Option key={type} value={type}>{type.toUpperCase()}</Option>
              ))}
            </Select>
          </Form.Item>
          <Form.Item name="jdbcUrl" label="JDBC URL">
            <Input placeholder="请输入JDBC URL" />
          </Form.Item>
          <Form.Item name="username" label="用户名">
            <Input placeholder="请输入用户名" />
          </Form.Item>
          <Form.Item name="password" label="密码">
            <Input.Password placeholder="请输入密码" />
          </Form.Item>
          <Form.Item name="alias" label="别名">
            <Input placeholder="请输入别名" />
          </Form.Item>
          <Form.Item name="description" label="描述">
            <Input.TextArea placeholder="请输入描述" rows={3} />
          </Form.Item>
          <Form.Item name="updateUser" label="更新人">
            <Input placeholder="更新人" />
          </Form.Item>
        </Form>
      </Modal>

      {/* 详情模态框 */}
      <Modal
        title="数据库服务器详情"
        open={detailModalVisible}
        onCancel={() => setDetailModalVisible(false)}
        footer={null}
        width={600}
      >
        {selectedServer && (
          <div>
            <Divider>基本信息</Divider>
            <Row gutter={16}>
              <Col span={12}>
                <strong>服务器名称:</strong> {selectedServer.name}
              </Col>
              <Col span={12}>
                <strong>数据库类型:</strong> {selectedServer.sourceType}
              </Col>
            </Row>
            <Row gutter={16} style={{ marginTop: 16 }}>
              <Col span={12}>
                <strong>服务器地址:</strong> {selectedServer.host}
              </Col>
              <Col span={12}>
                <strong>用户名:</strong> {selectedServer.username}
              </Col>
            </Row>
            <Row gutter={16} style={{ marginTop: 16 }}>
              <Col span={12}>
                <strong>别名:</strong> {selectedServer.alias || '-'}
              </Col>
              <Col span={12}>
                <strong>创建人:</strong> {selectedServer.createUser}
              </Col>
            </Row>
            <Row gutter={16} style={{ marginTop: 16 }}>
              <Col span={24}>
                <strong>JDBC URL:</strong> <Text code>{selectedServer.jdbcUrl}</Text>
              </Col>
            </Row>
            <Row gutter={16} style={{ marginTop: 16 }}>
              <Col span={24}>
                <strong>描述:</strong> {selectedServer.description || '-'}
              </Col>
            </Row>
            <Divider>时间信息</Divider>
            <Row gutter={16}>
              <Col span={12}>
                <strong>创建时间:</strong> {dayjs(selectedServer.createTime).format('YYYY-MM-DD HH:mm:ss')}
              </Col>
              <Col span={12}>
                <strong>更新时间:</strong> {selectedServer.updateTime ? dayjs(selectedServer.updateTime).format('YYYY-MM-DD HH:mm:ss') : '-'}
              </Col>
            </Row>
          </div>
        )}
      </Modal>
    </div>
  );
};

export default DbServerManagement; 