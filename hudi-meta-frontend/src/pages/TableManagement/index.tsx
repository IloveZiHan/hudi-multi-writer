import React, { useState, useEffect } from 'react';
import {
  Table,
  Button,
  Space,
  Input,
  Select,
  DatePicker,
  Card,
  Tag,
  Tooltip,
  Modal,
  message,
  Popconfirm,
  Row,
  Col,
  Statistic,
  Badge,
} from 'antd';
import {
  PlusOutlined,
  SearchOutlined,
  EditOutlined,
  DeleteOutlined,
  ExportOutlined,
  ReloadOutlined,
  EyeOutlined,
  CloudUploadOutlined,
  CloudDownloadOutlined,
} from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import { useRequest } from 'ahooks';
import dayjs from 'dayjs';
import tableApiService from '@services/tableApi';
import {
  MetaTableDTO,
  SearchCriteria,
  TableStatus,
  TableStatusLabels,
  TableStatusColors,
} from '@types/api';
import CreateTableModal from './CreateTableModal';
import EditTableModal from './EditTableModal';
import TableDetailModal from './TableDetailModal';
import './index.less';

const { Option } = Select;
const { RangePicker } = DatePicker;

/**
 * 表管理页面
 */
const TableManagement: React.FC = () => {
  // 状态管理
  const [searchForm, setSearchForm] = useState<SearchCriteria>({});
  const [selectedRowKeys, setSelectedRowKeys] = useState<string[]>([]);
  const [selectedRows, setSelectedRows] = useState<MetaTableDTO[]>([]);
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(10);
  
  // 模态框状态
  const [createModalVisible, setCreateModalVisible] = useState(false);
  const [editModalVisible, setEditModalVisible] = useState(false);
  const [detailModalVisible, setDetailModalVisible] = useState(false);
  const [currentRecord, setCurrentRecord] = useState<MetaTableDTO | null>(null);

  // 获取表列表数据
  const {
    data: tableData,
    loading: tableLoading,
    run: fetchTables,
  } = useRequest(
    () => tableApiService.getAllTables(currentPage - 1, pageSize),
    {
      refreshDeps: [currentPage, pageSize],
      onError: (error) => {
        message.error(`获取表列表失败: ${error.message}`);
      },
    }
  );

  // 获取统计数据
  const { data: statsData, run: fetchStats } = useRequest(
    () => tableApiService.getTableStatusStats(),
    {
      onError: (error) => {
        message.error(`获取统计数据失败: ${error.message}`);
      },
    }
  );

  // 表格列定义
  const columns: ColumnsType<MetaTableDTO> = [
    {
      title: '表ID',
      dataIndex: 'id',
      key: 'id',
      width: 200,
      fixed: 'left',
      render: (text: string, record: MetaTableDTO) => (
        <Button
          type="link"
          size="small"
          onClick={() => handleViewDetail(record)}
        >
          {text}
        </Button>
      ),
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      width: 100,
      render: (status: number) => (
        <Tag color={TableStatusColors[status as TableStatus]}>
          {TableStatusLabels[status as TableStatus]}
        </Tag>
      ),
    },
    {
      title: '分区',
      dataIndex: 'isPartitioned',
      key: 'isPartitioned',
      width: 80,
      render: (isPartitioned: boolean) => (
        <Badge
          status={isPartitioned ? 'success' : 'default'}
          text={isPartitioned ? '是' : '否'}
        />
      ),
    },
    {
      title: '源库',
      dataIndex: 'sourceDb',
      key: 'sourceDb',
      width: 120,
    },
    {
      title: '源表',
      dataIndex: 'sourceTable',
      key: 'sourceTable',
      width: 150,
    },
    {
      title: '目标库',
      dataIndex: 'targetDb',
      key: 'targetDb',
      width: 120,
    },
    {
      title: '目标表',
      dataIndex: 'targetTable',
      key: 'targetTable',
      width: 150,
    },
    {
      title: '标签',
      dataIndex: 'tags',
      key: 'tags',
      width: 150,
      render: (tags: string) => {
        if (!tags) return '-';
        return tags.split(',').map(tag => (
          <Tag key={tag} size="small">
            {tag.trim()}
          </Tag>
        ));
      },
    },
    {
      title: '创建时间',
      dataIndex: 'createdTime',
      key: 'createdTime',
      width: 160,
      render: (time: string) => dayjs(time).format('YYYY-MM-DD HH:mm:ss'),
    },
    {
      title: '更新时间',
      dataIndex: 'updatedTime',
      key: 'updatedTime',
      width: 160,
      render: (time: string) => dayjs(time).format('YYYY-MM-DD HH:mm:ss'),
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
            />
          </Tooltip>
          <Tooltip title="编辑">
            <Button
              type="text"
              icon={<EditOutlined />}
              onClick={() => handleEdit(record)}
            />
          </Tooltip>
          <Tooltip title={record.status === TableStatus.ONLINE ? '下线' : '上线'}>
            <Button
              type="text"
              icon={record.status === TableStatus.ONLINE ? <CloudDownloadOutlined /> : <CloudUploadOutlined />}
              onClick={() => handleToggleStatus(record)}
            />
          </Tooltip>
          <Tooltip title="删除">
            <Popconfirm
              title="确定要删除这个表吗？"
              onConfirm={() => handleDelete(record)}
              okText="确定"
              cancelText="取消"
            >
              <Button type="text" danger icon={<DeleteOutlined />} />
            </Popconfirm>
          </Tooltip>
        </Space>
      ),
    },
  ];

  // 表格行选择配置
  const rowSelection = {
    selectedRowKeys,
    onChange: (keys: React.Key[], rows: MetaTableDTO[]) => {
      setSelectedRowKeys(keys as string[]);
      setSelectedRows(rows);
    },
    onSelectAll: (selected: boolean, selectedRows: MetaTableDTO[], changeRows: MetaTableDTO[]) => {
      console.log('onSelectAll:', selected, selectedRows, changeRows);
    },
  };

  // 处理搜索
  const handleSearch = () => {
    setCurrentPage(1);
    fetchTables();
  };

  // 处理重置
  const handleReset = () => {
    setSearchForm({});
    setCurrentPage(1);
    fetchTables();
  };

  // 处理创建
  const handleCreate = () => {
    setCreateModalVisible(true);
  };

  // 处理编辑
  const handleEdit = (record: MetaTableDTO) => {
    setCurrentRecord(record);
    setEditModalVisible(true);
  };

  // 处理查看详情
  const handleViewDetail = (record: MetaTableDTO) => {
    setCurrentRecord(record);
    setDetailModalVisible(true);
  };

  // 处理删除
  const handleDelete = async (record: MetaTableDTO) => {
    try {
      await tableApiService.deleteTable(record.id);
      message.success('删除成功');
      fetchTables();
      fetchStats();
    } catch (error) {
      message.error(`删除失败: ${error}`);
    }
  };

  // 处理状态切换
  const handleToggleStatus = async (record: MetaTableDTO) => {
    try {
      if (record.status === TableStatus.ONLINE) {
        await tableApiService.offlineTable(record.id);
        message.success('下线成功');
      } else {
        await tableApiService.onlineTable(record.id);
        message.success('上线成功');
      }
      fetchTables();
      fetchStats();
    } catch (error) {
      message.error(`操作失败: ${error}`);
    }
  };

  // 处理批量删除
  const handleBatchDelete = async () => {
    if (selectedRowKeys.length === 0) {
      message.warning('请选择要删除的表');
      return;
    }

    Modal.confirm({
      title: '确认删除',
      content: `确定要删除选中的 ${selectedRowKeys.length} 个表吗？`,
      onOk: async () => {
        try {
          await tableApiService.batchDeleteTables(selectedRowKeys);
          message.success('批量删除成功');
          setSelectedRowKeys([]);
          setSelectedRows([]);
          fetchTables();
          fetchStats();
        } catch (error) {
          message.error(`批量删除失败: ${error}`);
        }
      },
    });
  };

  // 处理批量上线
  const handleBatchOnline = async () => {
    if (selectedRowKeys.length === 0) {
      message.warning('请选择要上线的表');
      return;
    }

    try {
      await tableApiService.batchOnlineTables(selectedRowKeys);
      message.success('批量上线成功');
      setSelectedRowKeys([]);
      setSelectedRows([]);
      fetchTables();
      fetchStats();
    } catch (error) {
      message.error(`批量上线失败: ${error}`);
    }
  };

  // 处理批量下线
  const handleBatchOffline = async () => {
    if (selectedRowKeys.length === 0) {
      message.warning('请选择要下线的表');
      return;
    }

    try {
      await tableApiService.batchOfflineTables(selectedRowKeys);
      message.success('批量下线成功');
      setSelectedRowKeys([]);
      setSelectedRows([]);
      fetchTables();
      fetchStats();
    } catch (error) {
      message.error(`批量下线失败: ${error}`);
    }
  };

  // 处理导出
  const handleExport = async () => {
    try {
      if (selectedRowKeys.length > 0) {
        await tableApiService.exportTables(selectedRowKeys);
        message.success('导出成功');
      } else {
        await tableApiService.exportSearchResults(searchForm);
        message.success('导出成功');
      }
    } catch (error) {
      message.error(`导出失败: ${error}`);
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
    fetchTables();
    fetchStats();
  };

  // 处理编辑成功
  const handleEditSuccess = () => {
    setEditModalVisible(false);
    setCurrentRecord(null);
    fetchTables();
    fetchStats();
  };

  return (
    <div className="table-management">
      {/* 统计卡片 */}
      <Row gutter={16} className="stats-cards">
        <Col span={6}>
          <Card>
            <Statistic title="总表数" value={statsData?.total || 0} />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic 
              title="已上线" 
              value={statsData?.online || 0}
              valueStyle={{ color: '#3f8600' }}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic 
              title="未上线" 
              value={statsData?.offline || 0}
              valueStyle={{ color: '#cf1322' }}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic 
              title="分区表" 
              value={statsData?.partitioned || 0}
              valueStyle={{ color: '#1890ff' }}
            />
          </Card>
        </Col>
      </Row>

      {/* 搜索表单 */}
      <Card className="search-form" title="搜索条件">
        <Row gutter={16}>
          <Col span={6}>
            <Input
              placeholder="表ID或关键词"
              value={searchForm.keyword}
              onChange={(e) => setSearchForm({ ...searchForm, keyword: e.target.value })}
              allowClear
            />
          </Col>
          <Col span={4}>
            <Select
              placeholder="状态"
              value={searchForm.status}
              onChange={(value) => setSearchForm({ ...searchForm, status: value })}
              allowClear
            >
              <Option value={TableStatus.OFFLINE}>未上线</Option>
              <Option value={TableStatus.ONLINE}>已上线</Option>
            </Select>
          </Col>
          <Col span={4}>
            <Select
              placeholder="分区"
              value={searchForm.isPartitioned}
              onChange={(value) => setSearchForm({ ...searchForm, isPartitioned: value })}
              allowClear
            >
              <Option value={true}>是</Option>
              <Option value={false}>否</Option>
            </Select>
          </Col>
          <Col span={6}>
            <RangePicker
              placeholder={['开始时间', '结束时间']}
              onChange={(dates) => {
                setSearchForm({
                  ...searchForm,
                  createdTimeStart: dates?.[0]?.format('YYYY-MM-DD HH:mm:ss'),
                  createdTimeEnd: dates?.[1]?.format('YYYY-MM-DD HH:mm:ss'),
                });
              }}
            />
          </Col>
          <Col span={4}>
            <Space>
              <Button type="primary" icon={<SearchOutlined />} onClick={handleSearch}>
                搜索
              </Button>
              <Button onClick={handleReset}>重置</Button>
            </Space>
          </Col>
        </Row>
      </Card>

      {/* 操作按钮 */}
      <Card className="action-buttons">
        <Row justify="space-between">
          <Col>
            <Space>
              <Button
                type="primary"
                icon={<PlusOutlined />}
                onClick={handleCreate}
              >
                创建表
              </Button>
              <Button
                icon={<DeleteOutlined />}
                danger
                onClick={handleBatchDelete}
                disabled={selectedRowKeys.length === 0}
              >
                批量删除
              </Button>
              <Button
                icon={<CloudUploadOutlined />}
                onClick={handleBatchOnline}
                disabled={selectedRowKeys.length === 0}
              >
                批量上线
              </Button>
              <Button
                icon={<CloudDownloadOutlined />}
                onClick={handleBatchOffline}
                disabled={selectedRowKeys.length === 0}
              >
                批量下线
              </Button>
              <Button
                icon={<ExportOutlined />}
                onClick={handleExport}
              >
                导出
              </Button>
            </Space>
          </Col>
          <Col>
            <Space>
              <span>已选择 {selectedRowKeys.length} 项</span>
              <Button
                icon={<ReloadOutlined />}
                onClick={() => {
                  fetchTables();
                  fetchStats();
                }}
              >
                刷新
              </Button>
            </Space>
          </Col>
        </Row>
      </Card>

      {/* 表格 */}
      <Card>
        <Table
          columns={columns}
          dataSource={tableData?.data || []}
          rowKey="id"
          loading={tableLoading}
          rowSelection={rowSelection}
          scroll={{ x: 1500 }}
          pagination={{
            current: currentPage,
            pageSize: pageSize,
            total: tableData?.total || 0,
            showSizeChanger: true,
            showQuickJumper: true,
            showTotal: (total, range) =>
              `第 ${range[0]}-${range[1]} 条，共 ${total} 条`,
            pageSizeOptions: ['10', '20', '50', '100'],
          }}
          onChange={handleTableChange}
        />
      </Card>

      {/* 创建表模态框 */}
      <CreateTableModal
        visible={createModalVisible}
        onCancel={() => setCreateModalVisible(false)}
        onSuccess={handleCreateSuccess}
      />

      {/* 编辑表模态框 */}
      <EditTableModal
        visible={editModalVisible}
        record={currentRecord}
        onCancel={() => {
          setEditModalVisible(false);
          setCurrentRecord(null);
        }}
        onSuccess={handleEditSuccess}
      />

      {/* 表详情模态框 */}
      <TableDetailModal
        visible={detailModalVisible}
        record={currentRecord}
        onCancel={() => {
          setDetailModalVisible(false);
          setCurrentRecord(null);
        }}
      />
    </div>
  );
};

export default TableManagement; 