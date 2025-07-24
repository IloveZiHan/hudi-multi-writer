import React, { useState, useEffect } from 'react';
import {
  Table,
  Button,
  Space,
  Input,
  DatePicker,
  Card,
  Tooltip,
  Modal,
  message,
  Popconfirm,
  Row,
  Col,
  Statistic,
  Typography,
} from 'antd';
import {
  PlusOutlined,
  SearchOutlined,
  EditOutlined,
  DeleteOutlined,
  ExportOutlined,
  ReloadOutlined,
  EyeOutlined,
  AppstoreOutlined,
  TableOutlined,
  CodeOutlined,
} from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import { useRequest } from 'ahooks';
import dayjs from 'dayjs';
import applicationApiService from '@services/applicationApi';
import {
  ApplicationDTO,
  ApplicationSearchCriteria,
} from '@services/applicationApi';
import CreateApplicationModal from './CreateApplicationModal';
import EditApplicationModal from './EditApplicationModal';
import ApplicationDetailModal from './ApplicationDetailModal';
import TableManagementModal from './TableManagementModal';
import './index.less';

const { RangePicker } = DatePicker;
const { Title, Text } = Typography;

/**
 * 应用程序管理页面
 */
const ApplicationManagement: React.FC = () => {
  // 状态管理
  const [searchForm, setSearchForm] = useState<ApplicationSearchCriteria>({});
  const [selectedRowKeys, setSelectedRowKeys] = useState<number[]>([]);
  const [selectedRows, setSelectedRows] = useState<ApplicationDTO[]>([]);
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(10);
  
  // 模态框状态
  const [createModalVisible, setCreateModalVisible] = useState(false);
  const [editModalVisible, setEditModalVisible] = useState(false);
  const [detailModalVisible, setDetailModalVisible] = useState(false);
  const [tableManagementModalVisible, setTableManagementModalVisible] = useState(false);
  const [currentRecord, setCurrentRecord] = useState<ApplicationDTO | null>(null);
  
  // 操作loading状态
  const [operationLoading, setOperationLoading] = useState<{
    [key: string]: boolean;
  }>({});
  const [batchLoading, setBatchLoading] = useState({
    delete: false,
  });
  const [refreshLoading, setRefreshLoading] = useState(false);
  const [searchLoading, setSearchLoading] = useState(false);
  const [resetLoading, setResetLoading] = useState(false);

  // 获取应用程序列表数据
  const {
    data: applicationData,
    loading: applicationLoading,
    run: fetchApplications,
  } = useRequest(
    () => applicationApiService.getAllApplications(currentPage - 1, pageSize),
    {
      refreshDeps: [currentPage, pageSize],
      onError: (error) => {
        message.error(`获取应用程序列表失败: ${error.message}`);
      },
    }
  );

  // 获取统计数据
  const { data: statsData, run: fetchStats } = useRequest(
    () => applicationApiService.getApplicationStats(),
    {
      onError: (error) => {
        message.error(`获取统计数据失败: ${error.message}`);
      },
    }
  );

  // 表格列定义
  const columns: ColumnsType<ApplicationDTO> = [
    {
      title: 'ID',
      dataIndex: 'id',
      key: 'id',
      width: 80,
      fixed: 'left',
    },
    {
      title: '应用名称',
      dataIndex: 'name',
      key: 'name',
      width: 200,
      ellipsis: {
        showTitle: true,
      },
      render: (text: string, record: ApplicationDTO) => (
        <Button
          type="link"
          size="small"
          onClick={() => handleViewDetail(record)}
          style={{ textAlign: 'left', padding: 0 }}
        >
          {text}
        </Button>
      ),
    },
    {
      title: '描述',
      dataIndex: 'description',
      key: 'description',
      width: 300,
      ellipsis: {
        showTitle: true,
      },
      render: (text: string) => text || '-',
    },
    {
      title: '创建时间',
      dataIndex: 'createTime',
      key: 'createTime',
      width: 180,
      render: (text: string) => dayjs(text).format('YYYY-MM-DD HH:mm:ss'),
    },
    {
      title: '更新时间',
      dataIndex: 'updateTime',
      key: 'updateTime',
      width: 180,
      render: (text: string) => dayjs(text).format('YYYY-MM-DD HH:mm:ss'),
    },
    {
      title: '操作',
      key: 'action',
      width: 280,
      fixed: 'right',
      render: (_, record) => (
        <Space size="small">
          <Tooltip title="查看详情">
            <Button
              type="text"
              icon={<EyeOutlined />}
              onClick={() => handleViewDetail(record)}
            />
          </Tooltip>
          
          <Tooltip title="编辑">
            <Button
              type="text"
              icon={<EditOutlined />}
              onClick={() => handleEdit(record)}
            />
          </Tooltip>
          
          <Tooltip title="表管理">
            <Button
              type="text"
              icon={<TableOutlined />}
              onClick={() => handleTableManagement(record)}
            />
          </Tooltip>
          
          <Tooltip title="删除">
            <Popconfirm
              title="确定要删除这个应用程序吗？"
              onConfirm={() => handleDelete(record)}
              okText="确定"
              cancelText="取消"
            >
              <Button 
                type="text" 
                danger 
                icon={<DeleteOutlined />}
                loading={operationLoading[`delete_${record.id}`]}
              />
            </Popconfirm>
          </Tooltip>
        </Space>
      ),
    },
  ];

  // 表格行选择配置
  const rowSelection = {
    selectedRowKeys,
    onChange: (keys: React.Key[], rows: ApplicationDTO[]) => {
      setSelectedRowKeys(keys as number[]);
      setSelectedRows(rows);
    },
  };

  // 处理搜索
  const handleSearch = async () => {
    setSearchLoading(true);
    try {
      setCurrentPage(1);
      await fetchApplications();
    } finally {
      setSearchLoading(false);
    }
  };

  // 处理重置
  const handleReset = async () => {
    setResetLoading(true);
    try {
      setSearchForm({});
      setCurrentPage(1);
      await fetchApplications();
    } finally {
      setResetLoading(false);
    }
  };

  // 处理创建
  const handleCreate = () => {
    setCreateModalVisible(true);
  };

  // 处理编辑
  const handleEdit = (record: ApplicationDTO) => {
    setCurrentRecord(record);
    setEditModalVisible(true);
  };

  // 处理查看详情
  const handleViewDetail = (record: ApplicationDTO) => {
    setCurrentRecord(record);
    setDetailModalVisible(true);
  };

  // 处理表管理
  const handleTableManagement = (record: ApplicationDTO) => {
    setCurrentRecord(record);
    setTableManagementModalVisible(true);
  };

  // 处理删除
  const handleDelete = async (record: ApplicationDTO) => {
    const key = `delete_${record.id}`;
    setOperationLoading(prev => ({ ...prev, [key]: true }));
    try {
      await applicationApiService.deleteApplication(record.id);
      message.success('删除成功');
      fetchApplications();
      fetchStats();
    } catch (error) {
      message.error(`删除失败: ${error}`);
    } finally {
      setOperationLoading(prev => ({ ...prev, [key]: false }));
    }
  };

  // 处理批量删除
  const handleBatchDelete = async () => {
    if (selectedRowKeys.length === 0) {
      message.warning('请选择要删除的应用程序');
      return;
    }

    setBatchLoading(prev => ({ ...prev, delete: true }));
    try {
      await applicationApiService.batchDeleteApplications(selectedRowKeys);
      message.success('批量删除成功');
      setSelectedRowKeys([]);
      setSelectedRows([]);
      fetchApplications();
      fetchStats();
    } catch (error) {
      message.error(`批量删除失败: ${error}`);
    } finally {
      setBatchLoading(prev => ({ ...prev, delete: false }));
    }
  };

  // 处理页面变化
  const handleTableChange = (pagination: any) => {
    setCurrentPage(pagination.current);
    setPageSize(pagination.pageSize);
  };

  // 处理创建成功
  const handleCreateSuccess = () => {
    setCreateModalVisible(false);
    message.success('应用程序创建成功！');
    fetchApplications();
    fetchStats();
  };

  // 处理编辑成功
  const handleEditSuccess = () => {
    setEditModalVisible(false);
    setCurrentRecord(null);
    fetchApplications();
    fetchStats();
  };

  // 处理刷新
  const handleRefresh = async () => {
    setRefreshLoading(true);
    try {
      await Promise.all([fetchApplications(), fetchStats()]);
    } finally {
      setRefreshLoading(false);
    }
  };

  // 渲染搜索表单
  const renderSearchForm = () => (
    <Card className="search-form" title="搜索条件">
      <Row gutter={16}>
        <Col span={6}>
          <Input
            placeholder="应用名称或关键词"
            value={searchForm.keyword}
            onChange={(e) => setSearchForm({ ...searchForm, keyword: e.target.value })}
            allowClear
          />
        </Col>
        <Col span={6}>
          <Input
            placeholder="应用名称"
            value={searchForm.name}
            onChange={(e) => setSearchForm({ ...searchForm, name: e.target.value })}
            allowClear
          />
        </Col>
        <Col span={6}>
          <RangePicker
            placeholder={['开始时间', '结束时间']}
            onChange={(dates) => {
              setSearchForm({
                ...searchForm,
                createTimeStart: dates?.[0]?.format('YYYY-MM-DD HH:mm:ss'),
                createTimeEnd: dates?.[1]?.format('YYYY-MM-DD HH:mm:ss'),
              });
            }}
          />
        </Col>
        <Col span={6}>
          <Space>
            <Button 
              type="primary" 
              icon={<SearchOutlined />} 
              onClick={handleSearch}
              loading={searchLoading}
            >
              搜索
            </Button>
            <Button onClick={handleReset} loading={resetLoading}>重置</Button>
          </Space>
        </Col>
      </Row>
    </Card>
  );

  // 渲染操作按钮
  const renderActionButtons = () => (
    <Card className="action-buttons">
      <Row justify="space-between">
        <Col>
          <Space>
            <Button
              type="primary"
              icon={<PlusOutlined />}
              onClick={handleCreate}
            >
              创建应用程序
            </Button>
            <Button
              icon={<DeleteOutlined />}
              danger
              onClick={handleBatchDelete}
              disabled={selectedRowKeys.length === 0}
              loading={batchLoading.delete}
            >
              批量删除
            </Button>
          </Space>
        </Col>
        <Col>
          <Button
            icon={<ReloadOutlined />}
            onClick={handleRefresh}
            loading={refreshLoading}
          >
            刷新
          </Button>
        </Col>
      </Row>
    </Card>
  );

  return (
    <div className="application-management">
      {/* 页面标题 */}
      <div className="page-header">
        <Title level={2}>
          <AppstoreOutlined />
          应用程序管理
        </Title>
        <Text type="secondary">
          管理应用程序的创建、编辑、删除和表关联配置
        </Text>
      </div>

      {/* 统计卡片 */}
      <Row gutter={16} className="stats-cards">
        <Col span={8}>
          <Card>
            <Statistic title="总应用数" value={statsData?.total || 0} />
          </Card>
        </Col>
        <Col span={8}>
          <Card>
            <Statistic 
              title="活跃应用" 
              value={statsData?.active || 0}
              valueStyle={{ color: '#3f8600' }}
            />
          </Card>
        </Col>
        <Col span={8}>
          <Card>
            <Statistic 
              title="关联表数" 
              value={statsData?.totalTables || 0}
              valueStyle={{ color: '#1890ff' }}
            />
          </Card>
        </Col>
      </Row>

      {/* 搜索表单 */}
      {renderSearchForm()}

      {/* 操作按钮 */}
      {renderActionButtons()}

      {/* 表格 */}
      <Card>
        <Table
          columns={columns}
          dataSource={applicationData?.data || []}
          rowKey="id"
          loading={applicationLoading}
          rowSelection={rowSelection}
          scroll={{ x: 1200 }}
          pagination={{
            current: currentPage,
            pageSize: pageSize,
            total: applicationData?.total || 0,
            showSizeChanger: true,
            showQuickJumper: true,
            showTotal: (total, range) =>
              `第 ${range[0]}-${range[1]} 条，共 ${total} 条`,
            pageSizeOptions: ['10', '20', '50', '100'],
          }}
          onChange={handleTableChange}
        />
      </Card>

      {/* 创建应用程序模态框 */}
      <CreateApplicationModal
        visible={createModalVisible}
        onCancel={() => setCreateModalVisible(false)}
        onSuccess={handleCreateSuccess}
      />

      {/* 编辑应用程序模态框 */}
      <EditApplicationModal
        visible={editModalVisible}
        record={currentRecord}
        onCancel={() => {
          setEditModalVisible(false);
          setCurrentRecord(null);
        }}
        onSuccess={handleEditSuccess}
      />

      {/* 应用程序详情模态框 */}
      <ApplicationDetailModal
        visible={detailModalVisible}
        record={currentRecord}
        onCancel={() => {
          setDetailModalVisible(false);
          setCurrentRecord(null);
        }}
      />

      {/* 表管理模态框 */}
      <TableManagementModal
        visible={tableManagementModalVisible}
        application={currentRecord}
        onCancel={() => {
          setTableManagementModalVisible(false);
          setCurrentRecord(null);
        }}
      />
    </div>
  );
};

export default ApplicationManagement; 