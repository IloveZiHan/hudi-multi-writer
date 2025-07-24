import React, { useState } from 'react';
import {
  Modal,
  Table,
  Button,
  Space,
  Input,
  Select,
  Card,
  message,
  Popconfirm,
  Form,
  Row,
  Col,
  Tag,
  Tooltip,
  DatePicker,
} from 'antd';
import {
  TableOutlined,
  PlusOutlined,
  EditOutlined,
  DeleteOutlined,
  SearchOutlined,
  ReloadOutlined,
  EyeOutlined,
} from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import { useRequest } from 'ahooks';
import dayjs from 'dayjs';
import tableApplicationApiService from '@services/tableApplicationApi';
import tableApiService from '@services/tableApi';
import {
  TableApplicationDTO,
  TableApplicationSearchCriteria,
  CreateTableApplicationRequest,
  UpdateTableApplicationRequest,
} from '@services/tableApplicationApi';
import { ApplicationDTO } from '@services/applicationApi';
import { MetaTableDTO } from '@services/tableApi';

const { Option } = Select;
const { RangePicker } = DatePicker;

interface TableManagementModalProps {
  visible: boolean;
  application: ApplicationDTO | null;
  onCancel: () => void;
}

/**
 * 表管理模态框组件
 */
const TableManagementModal: React.FC<TableManagementModalProps> = ({
  visible,
  application,
  onCancel,
}) => {
  const [searchForm, setSearchForm] = useState<TableApplicationSearchCriteria>({});
  const [selectedRowKeys, setSelectedRowKeys] = useState<number[]>([]);
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(10);
  const [createFormVisible, setCreateFormVisible] = useState(false);
  const [editFormVisible, setEditFormVisible] = useState(false);
  const [currentRecord, setCurrentRecord] = useState<TableApplicationDTO | null>(null);
  
  const [createForm] = Form.useForm();
  const [editForm] = Form.useForm();

  // 获取表应用关联列表
  const {
    data: tableApplicationData,
    loading: tableApplicationLoading,
    run: fetchTableApplications,
  } = useRequest(
    () => {
      if (!application) return Promise.resolve({ data: [], total: 0 });
      const criteria = {
        ...searchForm,
        applicationName: application.name,
      };
      return tableApplicationApiService.searchTableApplications(criteria, currentPage - 1, pageSize);
    },
    {
      refreshDeps: [currentPage, pageSize, application],
      ready: !!application,
      onError: (error) => {
        message.error(`获取表关联列表失败: ${error.message}`);
      },
    }
  );

  // 获取所有表列表（用于创建关联时选择）
  const { data: allTablesData } = useRequest(
    () => tableApiService.getAllTables(0, 1000),
    {
      onError: (error) => {
        message.error(`获取表列表失败: ${error.message}`);
      },
    }
  );



  // 创建表关联
  const { loading: createLoading, run: createTableApplication } = useRequest(
    tableApplicationApiService.createTableApplication,
    {
      manual: true,
      onSuccess: () => {
        message.success('表关联创建成功！');
        setCreateFormVisible(false);
        createForm.resetFields();
        fetchTableApplications();
      },
      onError: (error) => {
        message.error(`创建失败: ${error.message}`);
      },
    }
  );

  // 更新表关联
  const { loading: updateLoading, run: updateTableApplication } = useRequest(
    (id: number, request: UpdateTableApplicationRequest) =>
      tableApplicationApiService.updateTableApplication(id, request),
    {
      manual: true,
      onSuccess: () => {
        message.success('表关联更新成功！');
        setEditFormVisible(false);
        setCurrentRecord(null);
        editForm.resetFields();
        fetchTableApplications();
      },
      onError: (error) => {
        message.error(`更新失败: ${error.message}`);
      },
    }
  );

  // 删除表关联
  const { run: deleteTableApplication } = useRequest(
    tableApplicationApiService.deleteTableApplication,
    {
      manual: true,
      onSuccess: () => {
        message.success('删除成功');
        fetchTableApplications();
      },
      onError: (error) => {
        message.error(`删除失败: ${error.message}`);
      },
    }
  );

  // 批量删除
  const { run: batchDeleteTableApplications } = useRequest(
    tableApplicationApiService.batchDeleteTableApplications,
    {
      manual: true,
      onSuccess: () => {
        message.success('批量删除成功');
        setSelectedRowKeys([]);
        fetchTableApplications();
      },
      onError: (error) => {
        message.error(`批量删除失败: ${error.message}`);
      },
    }
  );

  // 表格列定义
  const columns: ColumnsType<TableApplicationDTO> = [
    {
      title: 'ID',
      dataIndex: 'id',
      key: 'id',
      width: 80,
    },
    {
      title: '表ID',
      dataIndex: 'tableId',
      key: 'tableId',
      width: 200,
      ellipsis: {
        showTitle: true,
      },
    },
    {
      title: '表类型',
      dataIndex: 'tableType',
      key: 'tableType',
      width: 120,
      render: (text: string) => (
        <Tag color={text === 'hudi' ? 'blue' : 'default'}>{text}</Tag>
      ),
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
      width: 160,
      fixed: 'right',
      render: (_, record) => (
        <Space size="small">
          <Tooltip title="编辑">
            <Button
              type="text"
              icon={<EditOutlined />}
              onClick={() => handleEdit(record)}
            />
          </Tooltip>
          
          <Tooltip title="删除">
            <Popconfirm
              title="确定要删除这个表关联吗？"
              onConfirm={() => deleteTableApplication(record.id)}
              okText="确定"
              cancelText="取消"
            >
              <Button 
                type="text" 
                danger 
                icon={<DeleteOutlined />}
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
    onChange: (keys: React.Key[]) => {
      setSelectedRowKeys(keys as number[]);
    },
  };

  // 处理搜索
  const handleSearch = () => {
    setCurrentPage(1);
    fetchTableApplications();
  };

  // 处理重置
  const handleReset = () => {
    setSearchForm({});
    setCurrentPage(1);
    fetchTableApplications();
  };

  // 处理创建
  const handleCreate = () => {
    setCreateFormVisible(true);
  };

  // 处理编辑
  const handleEdit = (record: TableApplicationDTO) => {
    setCurrentRecord(record);
    editForm.setFieldsValue({
      tableId: record.tableId,
      tableType: record.tableType,
    });
    setEditFormVisible(true);
  };

  // 处理创建提交
  const handleCreateSubmit = async () => {
    if (!application) return;

    try {
      const values = await createForm.validateFields();
      const request: CreateTableApplicationRequest = {
        tableId: values.tableId,
        applicationName: application.name,
        tableType: values.tableType,
      };
      await createTableApplication(request);
    } catch (error) {
      console.error('表单验证失败:', error);
    }
  };

  // 处理编辑提交
  const handleEditSubmit = async () => {
    if (!currentRecord) return;

    try {
      const values = await editForm.validateFields();
      const request: UpdateTableApplicationRequest = {
        tableId: values.tableId,
        tableType: values.tableType,
      };
      await updateTableApplication(currentRecord.id, request);
    } catch (error) {
      console.error('表单验证失败:', error);
    }
  };

  // 处理批量删除
  const handleBatchDelete = async () => {
    if (selectedRowKeys.length === 0) {
      message.warning('请选择要删除的表关联');
      return;
    }
    await batchDeleteTableApplications(selectedRowKeys);
  };

  // 处理页面变化
  const handleTableChange = (pagination: any) => {
    setCurrentPage(pagination.current);
    setPageSize(pagination.pageSize);
  };

  return (
    <Modal
      title={
        <Space>
          <TableOutlined />
          表管理 - {application?.name}
        </Space>
      }
      open={visible}
      onCancel={onCancel}
      footer={null}
      width={1200}
      destroyOnClose
    >
      {/* 搜索表单 */}
      <Card size="small" style={{ marginBottom: 16 }}>
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
              placeholder="表类型"
              value={searchForm.tableType}
              onChange={(value) => setSearchForm({ ...searchForm, tableType: value })}
              allowClear
            >
              <Option key="hudi" value="hudi">hudi</Option>
            </Select>
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
          <Col span={8}>
            <Space>
              <Button 
                type="primary" 
                icon={<SearchOutlined />} 
                onClick={handleSearch}
              >
                搜索
              </Button>
              <Button onClick={handleReset}>重置</Button>
              <Button
                type="primary"
                icon={<PlusOutlined />}
                onClick={handleCreate}
              >
                关联表
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
                onClick={fetchTableApplications}
              >
                刷新
              </Button>
            </Space>
          </Col>
        </Row>
      </Card>

      {/* 表格 */}
      <Table
        columns={columns}
        dataSource={tableApplicationData?.data || []}
        rowKey="id"
        loading={tableApplicationLoading}
        rowSelection={rowSelection}
        scroll={{ x: 1000 }}
        pagination={{
          current: currentPage,
          pageSize: pageSize,
          total: tableApplicationData?.total || 0,
          showSizeChanger: true,
          showQuickJumper: true,
          showTotal: (total, range) =>
            `第 ${range[0]}-${range[1]} 条，共 ${total} 条`,
          pageSizeOptions: ['10', '20', '50'],
        }}
        onChange={handleTableChange}
      />

      {/* 创建表关联模态框 */}
      <Modal
        title="关联表"
        open={createFormVisible}
        onCancel={() => {
          setCreateFormVisible(false);
          createForm.resetFields();
        }}
        footer={[
          <Button key="cancel" onClick={() => {
            setCreateFormVisible(false);
            createForm.resetFields();
          }}>
            取消
          </Button>,
          <Button 
            key="submit" 
            type="primary" 
            loading={createLoading} 
            onClick={handleCreateSubmit}
          >
            关联
          </Button>,
        ]}
        destroyOnClose
      >
        <Form form={createForm} layout="vertical">
          <Form.Item
            label="选择表"
            name="tableId"
            rules={[{ required: true, message: '请选择要关联的表' }]}
          >
            <Select
              placeholder="请选择表ID"
              showSearch
              filterOption={(input, option) =>
                (option?.children as unknown as string).toLowerCase().includes(input.toLowerCase())
              }
            >
              {allTablesData?.data?.map((table: MetaTableDTO) => (
                <Option key={table.id} value={table.id}>
                  {table.id}
                </Option>
              ))}
            </Select>
          </Form.Item>

          <Form.Item
            label="表类型"
            name="tableType"
            rules={[{ required: true, message: '请选择表类型' }]}
          >
            <Select placeholder="请选择表类型">
              <Option key="hudi" value="hudi">hudi</Option>
            </Select>
          </Form.Item>
        </Form>
      </Modal>

      {/* 编辑表关联模态框 */}
      <Modal
        title="编辑表关联"
        open={editFormVisible}
        onCancel={() => {
          setEditFormVisible(false);
          setCurrentRecord(null);
          editForm.resetFields();
        }}
        footer={[
          <Button key="cancel" onClick={() => {
            setEditFormVisible(false);
            setCurrentRecord(null);
            editForm.resetFields();
          }}>
            取消
          </Button>,
          <Button 
            key="submit" 
            type="primary" 
            loading={updateLoading} 
            onClick={handleEditSubmit}
          >
            更新
          </Button>,
        ]}
        destroyOnClose
      >
        <Form form={editForm} layout="vertical">
          <Form.Item
            label="选择表"
            name="tableId"
            rules={[{ required: true, message: '请选择要关联的表' }]}
          >
            <Select
              placeholder="请选择表ID"
              showSearch
              filterOption={(input, option) =>
                (option?.children as unknown as string).toLowerCase().includes(input.toLowerCase())
              }
            >
              {allTablesData?.data?.map((table: MetaTableDTO) => (
                <Option key={table.id} value={table.id}>
                  {table.id}
                </Option>
              ))}
            </Select>
          </Form.Item>

          <Form.Item
            label="表类型"
            name="tableType"
            rules={[{ required: true, message: '请选择表类型' }]}
          >
            <Select placeholder="请选择表类型">
              <Option key="hudi" value="hudi">hudi</Option>
            </Select>
          </Form.Item>
        </Form>
      </Modal>
    </Modal>
  );
};

export default TableManagementModal; 