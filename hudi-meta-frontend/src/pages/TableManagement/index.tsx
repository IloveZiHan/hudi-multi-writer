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
  Tabs,
  Alert,
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
  CloudUploadOutlined,
  CloudDownloadOutlined,
  RollbackOutlined,
  DatabaseOutlined,
  ImportOutlined,
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
} from '../../types/api';
import CreateTableModal from './CreateTableModal';
import EditTableModal from './EditTableModal';
import TableDetailModal from './TableDetailModal';
import ImportFromBusinessTableModal, { ImportTableData } from '../MetadataManagement/ImportFromBusinessTableModal';
import './index.less';

const { Option } = Select;
const { RangePicker } = DatePicker;
const { TabPane } = Tabs;
const { Title, Text } = Typography;

/**
 * 表管理页面
 */
const TableManagement: React.FC = () => {
  // 状态管理
  const [activeTab, setActiveTab] = useState('active'); // 当前活动的标签页
  const [searchForm, setSearchForm] = useState<SearchCriteria>({});
  const [deletedSearchForm, setDeletedSearchForm] = useState<SearchCriteria>({});
  const [selectedRowKeys, setSelectedRowKeys] = useState<string[]>([]);
  const [selectedRows, setSelectedRows] = useState<MetaTableDTO[]>([]);
  const [deletedSelectedRowKeys, setDeletedSelectedRowKeys] = useState<string[]>([]);
  const [deletedSelectedRows, setDeletedSelectedRows] = useState<MetaTableDTO[]>([]);
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(10);
  const [deletedCurrentPage, setDeletedCurrentPage] = useState(1);
  const [deletedPageSize, setDeletedPageSize] = useState(10);
  
  // 模态框状态
  const [createModalVisible, setCreateModalVisible] = useState(false);
  const [editModalVisible, setEditModalVisible] = useState(false);
  const [detailModalVisible, setDetailModalVisible] = useState(false);
  const [importModalVisible, setImportModalVisible] = useState(false);
  const [currentRecord, setCurrentRecord] = useState<MetaTableDTO | null>(null);
  const [importedTableData, setImportedTableData] = useState<ImportTableData | null>(null);
  
  // 操作loading状态
  const [operationLoading, setOperationLoading] = useState<{
    [key: string]: boolean;
  }>({});
  const [batchLoading, setBatchLoading] = useState<{
    delete: boolean;
    online: boolean;
    offline: boolean;
    export: boolean;
    restore: boolean;
    permanentDelete: boolean;
  }>({
    delete: false,
    online: false,
    offline: false,
    export: false,
    restore: false,
    permanentDelete: false,
  });
  const [refreshLoading, setRefreshLoading] = useState(false);
  const [searchLoading, setSearchLoading] = useState(false);
  const [resetLoading, setResetLoading] = useState(false);
  const [deletedSearchLoading, setDeletedSearchLoading] = useState(false);
  const [deletedResetLoading, setDeletedResetLoading] = useState(false);

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

  // 获取已删除表列表数据
  const {
    data: deletedTableData,
    loading: deletedTableLoading,
    run: fetchDeletedTables,
  } = useRequest(
    () => tableApiService.getDeletedTables(deletedCurrentPage - 1, deletedPageSize),
    {
      refreshDeps: [deletedCurrentPage, deletedPageSize],
      onError: (error) => {
        message.error(`获取已删除表列表失败: ${error.message}`);
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
      width: 280,
      fixed: 'left',
      ellipsis: {
        showTitle: true,
      },
      render: (text: string, record: MetaTableDTO) => (
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
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      width: 100,
      render: (status: number) => {
        let color = '#d9d9d9'; // 默认灰色
        let text = '未上线';
        
        if (status === TableStatus.ONLINE) {
          color = '#52c41a'; // 绿色
          text = '已上线';
        } else if (status === TableStatus.DELETED) {
          color = '#ff4d4f'; // 红色
          text = '已删除';
        }
        
        return (
          <div style={{ display: 'flex', alignItems: 'center' }}>
            <div
              style={{
                width: 8,
                height: 8,
                borderRadius: '50%',
                backgroundColor: color,
                marginRight: 8,
              }}
            />
            <span>{text}</span>
          </div>
        );
      },
    },
    {
      title: '分区',
      dataIndex: 'isPartitioned',
      key: 'isPartitioned',
      width: 80,
      render: (isPartitioned: boolean) => (
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
          {isPartitioned ? (
            <Tag color="blue" style={{ margin: 0 }}>
              <DatabaseOutlined style={{ marginRight: 4 }} />
              分区
            </Tag>
          ) : (
            <Tag color="default" style={{ margin: 0 }}>
              普通
            </Tag>
          )}
        </div>
      ),
    },
    {
      title: '源库',
      dataIndex: 'sourceDb',
      key: 'sourceDb',
      width: 120,
      ellipsis: {
        showTitle: true,
      },
    },
    {
      title: '源表',
      dataIndex: 'sourceTable',
      key: 'sourceTable',
      width: 150,
      ellipsis: {
        showTitle: true,
      },
    },
    {
      title: '标签',
      dataIndex: 'tags',
      key: 'tags',
      width: 150,
      ellipsis: {
        showTitle: true,
      },
      render: (tags: string) => {
        if (!tags) return '-';
        return tags.split(',').map(tag => (
          <Tag key={tag}>
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
      ellipsis: {
        showTitle: true,
      },
      render: (time: string) => dayjs(time).format('YYYY-MM-DD HH:mm:ss'),
    },
    {
      title: '更新时间',
      dataIndex: 'updatedTime',
      key: 'updatedTime',
      width: 160,
      ellipsis: {
        showTitle: true,
      },
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
              loading={operationLoading[`toggle_${record.id}`]}
            />
          </Tooltip>
          <Tooltip title="删除">
            <Popconfirm
              title="确定要删除这个表吗？"
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

  // 已删除表格列定义
  const deletedColumns: ColumnsType<MetaTableDTO> = [
    {
      title: '表ID',
      dataIndex: 'id',
      key: 'id',
      width: 280,
      fixed: 'left',
      ellipsis: {
        showTitle: true,
      },
      render: (text: string, record: MetaTableDTO) => (
        <Button
          type="link"
          size="small"
          onClick={() => handleViewDetail(record)}
          style={{ 
            textDecoration: 'line-through', 
            color: '#ff4d4f',
            opacity: 0.7,
            textAlign: 'left',
            padding: 0
          }}
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
        <div style={{ display: 'flex', alignItems: 'center' }}>
          <div
            style={{
              width: 8,
              height: 8,
              borderRadius: '50%',
              backgroundColor: '#ff4d4f',
              marginRight: 8,
              opacity: 0.8,
            }}
          />
          <span style={{ color: '#ff4d4f', opacity: 0.8 }}>已删除</span>
        </div>
      ),
    },
    {
      title: '分区',
      dataIndex: 'isPartitioned',
      key: 'isPartitioned',
      width: 80,
      render: (isPartitioned: boolean) => (
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
          {isPartitioned ? (
            <Tag color="blue" style={{ margin: 0, opacity: 0.7 }}>
              <DatabaseOutlined style={{ marginRight: 4 }} />
              分区
            </Tag>
          ) : (
            <Tag color="default" style={{ margin: 0, opacity: 0.7 }}>
              普通
            </Tag>
          )}
        </div>
      ),
    },
    {
      title: '源库',
      dataIndex: 'sourceDb',
      key: 'sourceDb',
      width: 120,
      ellipsis: {
        showTitle: true,
      },
      render: (text: string) => (
        <span style={{ 
          textDecoration: 'line-through', 
          color: '#8c8c8c',
          opacity: 0.7 
        }}>
          {text}
        </span>
      ),
    },
    {
      title: '源表',
      dataIndex: 'sourceTable',
      key: 'sourceTable',
      width: 150,
      ellipsis: {
        showTitle: true,
      },
      render: (text: string) => (
        <span style={{ 
          textDecoration: 'line-through', 
          color: '#8c8c8c',
          opacity: 0.7 
        }}>
          {text}
        </span>
      ),
    },
    {
      title: '标签',
      dataIndex: 'tags',
      key: 'tags',
      width: 150,
      ellipsis: {
        showTitle: true,
      },
      render: (tags: string) => {
        if (!tags) return <span style={{ opacity: 0.7 }}>-</span>;
        return tags.split(',').map(tag => (
          <Tag key={tag} style={{ opacity: 0.6, textDecoration: 'line-through' }}>
            {tag.trim()}
          </Tag>
        ));
      },
    },
    {
      title: '删除时间',
      dataIndex: 'updatedTime',
      key: 'updatedTime',
      width: 160,
      ellipsis: {
        showTitle: true,
      },
      render: (time: string) => (
        <span style={{ color: '#ff4d4f', fontWeight: 'bold' }}>
          {dayjs(time).format('YYYY-MM-DD HH:mm:ss')}
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
            />
          </Tooltip>
          <Tooltip title="恢复">
            <Button
              type="text"
              icon={<RollbackOutlined />}
              onClick={() => handleRestore(record)}
              loading={operationLoading[`restore_${record.id}`]}
              style={{ color: '#52c41a' }}
            />
          </Tooltip>
          <Tooltip title="永久删除">
            <Popconfirm
              title="确定要永久删除这个表吗？此操作不可恢复！"
              onConfirm={() => handlePermanentDelete(record)}
              okText="确定"
              cancelText="取消"
            >
              <Button 
                type="text" 
                danger 
                icon={<DeleteOutlined />}
                loading={operationLoading[`permanent_delete_${record.id}`]}
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
    onChange: (keys: React.Key[], rows: MetaTableDTO[]) => {
      setSelectedRowKeys(keys as string[]);
      setSelectedRows(rows);
    },
    onSelectAll: (selected: boolean, selectedRows: MetaTableDTO[], changeRows: MetaTableDTO[]) => {
      console.log('onSelectAll:', selected, selectedRows, changeRows);
    },
  };

  // 已删除表格行选择配置
  const deletedRowSelection = {
    selectedRowKeys: deletedSelectedRowKeys,
    onChange: (keys: React.Key[], rows: MetaTableDTO[]) => {
      setDeletedSelectedRowKeys(keys as string[]);
      setDeletedSelectedRows(rows);
    },
  };

  // 处理搜索
  const handleSearch = async () => {
    setSearchLoading(true);
    try {
      setCurrentPage(1);
      await fetchTables();
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
      await fetchTables();
    } finally {
      setResetLoading(false);
    }
  };

  // 处理创建
  const handleCreate = () => {
    setCreateModalVisible(true);
  };

  // 处理从业务表导入
  const handleImportFromBusinessTable = () => {
    setImportModalVisible(true);
  };

  // 处理导入完成
  const handleImportComplete = (importData: ImportTableData) => {
    setImportedTableData(importData);
    setImportModalVisible(false);
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
    const key = `delete_${record.id}`;
    setOperationLoading(prev => ({ ...prev, [key]: true }));
    try {
      await tableApiService.deleteTable(record.id);
      message.success('删除成功');
      // 刷新活跃表列表和已删除表列表
      fetchTables();
      fetchDeletedTables();
      fetchStats();
    } catch (error) {
      message.error(`删除失败: ${error}`);
    } finally {
      setOperationLoading(prev => ({ ...prev, [key]: false }));
    }
  };

  // 处理状态切换
  const handleToggleStatus = async (record: MetaTableDTO) => {
    const key = `toggle_${record.id}`;
    setOperationLoading(prev => ({ ...prev, [key]: true }));
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
    } finally {
      setOperationLoading(prev => ({ ...prev, [key]: false }));
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
        setBatchLoading(prev => ({ ...prev, delete: true }));
        try {
          await tableApiService.batchDeleteTables(selectedRowKeys);
          message.success('批量删除成功');
          setSelectedRowKeys([]);
          setSelectedRows([]);
          // 刷新活跃表列表和已删除表列表
          fetchTables();
          fetchDeletedTables();
          fetchStats();
        } catch (error) {
          message.error(`批量删除失败: ${error}`);
        } finally {
          setBatchLoading(prev => ({ ...prev, delete: false }));
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

    setBatchLoading(prev => ({ ...prev, online: true }));
    try {
      await tableApiService.batchOnlineTables(selectedRowKeys);
      message.success('批量上线成功');
      setSelectedRowKeys([]);
      setSelectedRows([]);
      fetchTables();
      fetchStats();
    } catch (error) {
      message.error(`批量上线失败: ${error}`);
    } finally {
      setBatchLoading(prev => ({ ...prev, online: false }));
    }
  };

  // 处理批量下线
  const handleBatchOffline = async () => {
    if (selectedRowKeys.length === 0) {
      message.warning('请选择要下线的表');
      return;
    }

    setBatchLoading(prev => ({ ...prev, offline: true }));
    try {
      await tableApiService.batchOfflineTables(selectedRowKeys);
      message.success('批量下线成功');
      setSelectedRowKeys([]);
      setSelectedRows([]);
      fetchTables();
      fetchStats();
    } catch (error) {
      message.error(`批量下线失败: ${error}`);
    } finally {
      setBatchLoading(prev => ({ ...prev, offline: false }));
    }
  };

  // 处理导出
  const handleExport = async () => {
    setBatchLoading(prev => ({ ...prev, export: true }));
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
    } finally {
      setBatchLoading(prev => ({ ...prev, export: false }));
    }
  };

  // 处理恢复
  const handleRestore = async (record: MetaTableDTO) => {
    const key = `restore_${record.id}`;
    setOperationLoading(prev => ({ ...prev, [key]: true }));
    try {
      await tableApiService.restoreTable(record.id);
      message.success('恢复成功');
      // 刷新活跃表列表和已删除表列表
      fetchTables();
      fetchDeletedTables();
      fetchStats();
    } catch (error) {
      message.error(`恢复失败: ${error}`);
    } finally {
      setOperationLoading(prev => ({ ...prev, [key]: false }));
    }
  };

  // 处理永久删除
  const handlePermanentDelete = async (record: MetaTableDTO) => {
    const key = `permanent_delete_${record.id}`;
    setOperationLoading(prev => ({ ...prev, [key]: true }));
    try {
      await tableApiService.permanentDeleteTable(record.id);
      message.success('永久删除成功');
      // 刷新已删除表列表
      fetchDeletedTables();
      fetchStats();
    } catch (error) {
      message.error(`永久删除失败: ${error}`);
    } finally {
      setOperationLoading(prev => ({ ...prev, [key]: false }));
    }
  };

  // 处理页面变化
  const handleTableChange = (pagination: any) => {
    setCurrentPage(pagination.current);
    setPageSize(pagination.pageSize);
  };

  // 处理已删除页面变化
  const handleDeletedTableChange = (pagination: any) => {
    setDeletedCurrentPage(pagination.current);
    setDeletedPageSize(pagination.pageSize);
  };

  // 处理创建成功
  const handleCreateSuccess = () => {
    setCreateModalVisible(false);
    setImportedTableData(null); // 清理导入数据
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

  // 处理已删除表搜索
  const handleDeletedSearch = async () => {
    setDeletedSearchLoading(true);
    try {
      setDeletedCurrentPage(1);
      await fetchDeletedTables();
    } finally {
      setDeletedSearchLoading(false);
    }
  };

  // 处理已删除表重置
  const handleDeletedReset = async () => {
    setDeletedResetLoading(true);
    try {
      setDeletedSearchForm({});
      setDeletedCurrentPage(1);
      await fetchDeletedTables();
    } finally {
      setDeletedResetLoading(false);
    }
  };

  // 处理批量恢复
  const handleBatchRestore = async () => {
    if (deletedSelectedRowKeys.length === 0) {
      message.warning('请选择要恢复的表');
      return;
    }

    Modal.confirm({
      title: '确认恢复',
      content: `确定要恢复选中的 ${deletedSelectedRowKeys.length} 个表吗？`,
      onOk: async () => {
        setBatchLoading(prev => ({ ...prev, restore: true }));
        try {
          await tableApiService.batchRestoreTables(deletedSelectedRowKeys);
          message.success('批量恢复成功');
          setDeletedSelectedRowKeys([]);
          setDeletedSelectedRows([]);
          fetchTables();
          fetchDeletedTables();
          fetchStats();
        } catch (error) {
          message.error(`批量恢复失败: ${error}`);
        } finally {
          setBatchLoading(prev => ({ ...prev, restore: false }));
        }
      },
    });
  };

  // 处理批量永久删除
  const handleBatchPermanentDelete = async () => {
    if (deletedSelectedRowKeys.length === 0) {
      message.warning('请选择要永久删除的表');
      return;
    }

    Modal.confirm({
      title: '确认永久删除',
      content: `确定要永久删除选中的 ${deletedSelectedRowKeys.length} 个表吗？此操作不可恢复！`,
      onOk: async () => {
        setBatchLoading(prev => ({ ...prev, permanentDelete: true }));
        try {
          await tableApiService.batchPermanentDeleteTables(deletedSelectedRowKeys);
          message.success('批量永久删除成功');
          setDeletedSelectedRowKeys([]);
          setDeletedSelectedRows([]);
          // 刷新已删除表列表
          fetchDeletedTables();
          fetchStats();
        } catch (error) {
          message.error(`批量永久删除失败: ${error}`);
        } finally {
          setBatchLoading(prev => ({ ...prev, permanentDelete: false }));
        }
      },
    });
  };

  // 处理刷新
  const handleRefresh = async () => {
    setRefreshLoading(true);
    try {
      // 始终刷新活跃表、已删除表和统计数据，确保数据一致性
      await Promise.all([fetchTables(), fetchDeletedTables(), fetchStats()]);
    } finally {
      setRefreshLoading(false);
    }
  };

  // 渲染活跃表的搜索表单
  const renderActiveSearchForm = () => (
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

  // 渲染已删除表的搜索表单
  const renderDeletedSearchForm = () => (
    <Card className="search-form" title="搜索条件">
      <Row gutter={16}>
        <Col span={6}>
          <Input
            placeholder="表ID或关键词"
            value={deletedSearchForm.keyword}
            onChange={(e) => setDeletedSearchForm({ ...deletedSearchForm, keyword: e.target.value })}
            allowClear
          />
        </Col>
        <Col span={4}>
          <Select
            placeholder="分区"
            value={deletedSearchForm.isPartitioned}
            onChange={(value) => setDeletedSearchForm({ ...deletedSearchForm, isPartitioned: value })}
            allowClear
          >
            <Option value={true}>是</Option>
            <Option value={false}>否</Option>
          </Select>
        </Col>
        <Col span={6}>
          <RangePicker
            placeholder={['删除开始时间', '删除结束时间']}
            onChange={(dates) => {
              setDeletedSearchForm({
                ...deletedSearchForm,
                createdTimeStart: dates?.[0]?.format('YYYY-MM-DD HH:mm:ss'),
                createdTimeEnd: dates?.[1]?.format('YYYY-MM-DD HH:mm:ss'),
              });
            }}
          />
        </Col>
        <Col span={8}>
          <Space>
            <Button 
              type="primary" 
              icon={<SearchOutlined />} 
              onClick={handleDeletedSearch}
              loading={deletedSearchLoading}
            >
              搜索
            </Button>
            <Button onClick={handleDeletedReset} loading={deletedResetLoading}>重置</Button>
          </Space>
        </Col>
      </Row>
    </Card>
  );

  // 渲染活跃表的操作按钮
  const renderActiveActionButtons = () => (
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
              icon={<ImportOutlined />}
              onClick={handleImportFromBusinessTable}
            >
              从业务表导入
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
            <Button
              icon={<CloudUploadOutlined />}
              onClick={handleBatchOnline}
              disabled={selectedRowKeys.length === 0}
              loading={batchLoading.online}
            >
              批量上线
            </Button>
            <Button
              icon={<CloudDownloadOutlined />}
              onClick={handleBatchOffline}
              disabled={selectedRowKeys.length === 0}
              loading={batchLoading.offline}
            >
              批量下线
            </Button>
            <Button
              icon={<ExportOutlined />}
              onClick={handleExport}
              loading={batchLoading.export}
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
              onClick={handleRefresh}
              loading={refreshLoading}
            >
              刷新
            </Button>
          </Space>
        </Col>
      </Row>
    </Card>
  );

  // 渲染已删除表的操作按钮
  const renderDeletedActionButtons = () => (
    <Card className="action-buttons">
      <Row justify="space-between">
        <Col>
          <Space>
            <Button
              icon={<RollbackOutlined />}
              onClick={handleBatchRestore}
              disabled={deletedSelectedRowKeys.length === 0}
              loading={batchLoading.restore}
            >
              批量恢复
            </Button>
            <Button
              icon={<DeleteOutlined />}
              danger
              onClick={handleBatchPermanentDelete}
              disabled={deletedSelectedRowKeys.length === 0}
              loading={batchLoading.permanentDelete}
            >
              批量永久删除
            </Button>
          </Space>
        </Col>
        <Col>
          <Space>
            <span>已选择 {deletedSelectedRowKeys.length} 项</span>
            <Button
              icon={<ReloadOutlined />}
              onClick={handleRefresh}
              loading={refreshLoading}
            >
              刷新
            </Button>
          </Space>
        </Col>
      </Row>
    </Card>
  );

  return (
    <div className="table-management">
      {/* 页面标题 */}
      <div className="page-header">
        <Title level={2}>
          <DatabaseOutlined />
          表管理
        </Title>
        <Text type="secondary">
          管理Hudi表的创建、编辑、删除和状态监控
        </Text>
      </div>

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

      {/* 表格 */}
      <Card>
        <Tabs defaultActiveKey="active" onChange={setActiveTab}>
          <Tabs.TabPane tab="活跃表" key="active">
            {renderActiveSearchForm()}
            {renderActiveActionButtons()}
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
          </Tabs.TabPane>
          <Tabs.TabPane 
            tab={
              <span style={{ color: '#ff4d4f' }}>
                <DeleteOutlined style={{ marginRight: 4 }} />
                已删除表
              </span>
            } 
            key="deleted"
                     >
            {/* 已删除表警告提示 */}
            <Alert
              message="已删除表"
              description="以下表已被标记为删除，您可以选择恢复或永久删除。永久删除后数据将无法恢复，请谨慎操作！"
              type="warning"
              showIcon
              style={{ 
                marginBottom: 16,
                border: '1px solid #ff4d4f',
                backgroundColor: '#fff1f0'
              }}
            />
            {renderDeletedSearchForm()}
            {renderDeletedActionButtons()}
            <Table
              columns={deletedColumns}
              dataSource={deletedTableData?.data || []}
              rowKey="id"
              loading={deletedTableLoading}
              rowSelection={deletedRowSelection}
              scroll={{ x: 1500 }}
              rowClassName={() => 'deleted-table-row'}
              pagination={{
                current: deletedCurrentPage,
                pageSize: deletedPageSize,
                total: deletedTableData?.total || 0,
                showSizeChanger: true,
                showQuickJumper: true,
                showTotal: (total, range) =>
                  `第 ${range[0]}-${range[1]} 条，共 ${total} 条`,
                pageSizeOptions: ['10', '20', '50', '100'],
              }}
              onChange={handleDeletedTableChange}
            />
          </Tabs.TabPane>
        </Tabs>
      </Card>

      {/* 创建表模态框 */}
      <CreateTableModal
        visible={createModalVisible}
        onCancel={() => {
          setCreateModalVisible(false);
          setImportedTableData(null); // 取消时也清理导入数据
        }}
        onSuccess={handleCreateSuccess}
        importData={importedTableData} // 传递导入数据
      />

      {/* 从业务表导入对话框 */}
      <ImportFromBusinessTableModal
        visible={importModalVisible}
        onCancel={() => setImportModalVisible(false)}
        onImport={handleImportComplete}
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