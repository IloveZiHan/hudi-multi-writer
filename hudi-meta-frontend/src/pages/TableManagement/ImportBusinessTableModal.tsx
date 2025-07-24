import React, { useState, useEffect } from 'react';
import {
  Modal,
  Form,
  Input,
  Select,
  Button,
  Row,
  Col,
  message,
  Space,
  Divider,
  Tag,
  Typography,
  Spin,
} from 'antd';
import {
  DatabaseOutlined,
  ImportOutlined,
} from '@ant-design/icons';
import { useRequest } from 'ahooks';
import dbServerApiService, { BusinessTableInfo, DbServerDTO } from '@services/dbServerApi';
import { TableStatus, MetaTableDTO } from '../../types/api';
import CreateTableModal from './CreateTableModal';
import './ImportBusinessTableModal.less';
import { generateTableId } from '../../config/databaseMapping';

const { Option } = Select;
const { Text } = Typography;

interface ImportBusinessTableModalProps {
  visible: boolean;
  onCancel: () => void;
  onSuccess: () => void;
}

/**
 * 从业务系统导入表模态框组件
 */
const ImportBusinessTableModal: React.FC<ImportBusinessTableModalProps> = ({
  visible,
  onCancel,
  onSuccess,
}) => {
  const [form] = Form.useForm();
  const [selectedServer, setSelectedServer] = useState<number | null>(null);
  const [selectedDatabase, setSelectedDatabase] = useState<string>('');
  const [businessTables, setBusinessTables] = useState<BusinessTableInfo[]>([]);
  const [selectedTable, setSelectedTable] = useState<BusinessTableInfo | null>(null);
  const [createModalVisible, setCreateModalVisible] = useState(false);
  const [createModalInitialData, setCreateModalInitialData] = useState<any>(null);

  // 获取数据库服务器列表
  const { data: dbServers, loading: dbServersLoading } = useRequest(
    () => dbServerApiService.getAllDbServers(0, 100),
    {
      onError: (error) => {
        message.error(`获取数据库服务器列表失败: ${error.message}`);
      },
    }
  );

  // 获取数据库列表
  const { data: databases, loading: databasesLoading, run: fetchDatabases } = useRequest(
    (serverId: number) => dbServerApiService.getDatabases(serverId),
    {
      manual: true,
      onError: (error) => {
        message.error(`获取数据库列表失败: ${error.message}`);
      },
    }
  );

  // 获取业务表列表
  const { loading: businessTablesLoading, run: fetchBusinessTables } = useRequest(
    (serverId: number, database: string) => dbServerApiService.getBusinessTables(serverId, database),
    {
      manual: true,
      onSuccess: (data) => {
        setBusinessTables(data);
      },
      onError: (error) => {
        message.error(`获取业务表列表失败: ${error.message}`);
      },
    }
  );

  // 重置状态
  const resetState = () => {
    setSelectedServer(null);
    setSelectedDatabase('');
    setBusinessTables([]);
    setSelectedTable(null);
    setCreateModalVisible(false);
    setCreateModalInitialData(null);
    form.resetFields();
  };

  // 当modal可见时，重置状态
  useEffect(() => {
    if (visible) {
      resetState();
    }
  }, [visible]);

  // 处理数据库服务器选择
  const handleServerChange = (serverId: number) => {
    setSelectedServer(serverId);
    setSelectedDatabase('');
    setBusinessTables([]);
    setSelectedTable(null);
    fetchDatabases(serverId);
  };

  // 处理数据库选择
  const handleDatabaseChange = (database: string) => {
    setSelectedDatabase(database);
    setBusinessTables([]);
    setSelectedTable(null);
    if (selectedServer) {
      fetchBusinessTables(selectedServer, database);
    }
  };

  // 处理表选择
  const handleTableSelect = (table: BusinessTableInfo) => {
    setSelectedTable(table);
  };

  // 获取表详细结构信息
  const { loading: tableSchemaLoading, run: fetchTableSchema } = useRequest(
    (serverId: number, database: string, tableName: string) => 
      dbServerApiService.getTableSchema(serverId, database, tableName),
    {
      manual: true,
      onSuccess: (schemaInfo) => {
        console.log('获取到表结构信息:', schemaInfo);
        // 使用详细的表结构信息创建schema
        const detailedSchema = createSchemaFromTableInfo(schemaInfo);
        proceedToCreateModal(detailedSchema);
      },
      onError: (error) => {
        console.warn('获取表详细结构失败，使用基础schema信息:', error);
        // 如果获取详细结构失败，使用基础schema信息
        proceedToCreateModal(selectedTable?.schema || '');
      },
    }
  );

  // 根据表结构信息创建标准的Spark Schema
  const createSchemaFromTableInfo = (tableInfo: any) => {
    if (!tableInfo?.fields || !Array.isArray(tableInfo.fields)) {
      return selectedTable?.schema || '';
    }

    const sparkFields = tableInfo.fields.map((field: any) => ({
      metadata: field.comment ? { comment: field.comment } : {},
      name: field.fieldName.toLowerCase(), // 字段名转小写
      nullable: field.isNullable !== false, // 默认可为空
      type: normalizeSparkDataType(field.fieldType), // 规范化数据类型
    }));

    const sparkSchema = {
      type: 'struct',
      fields: sparkFields,
    };

    return JSON.stringify(sparkSchema, null, 2);
  };

  // 规范化Spark数据类型
  const normalizeSparkDataType = (dbType: string): string => {
    const typeMap: Record<string, string> = {
      'int': 'integer',
      'bigint': 'long',
      'varchar': 'string',
      'text': 'string',
      'datetime': 'timestamp',
      'bool': 'boolean',
      'float': 'float',
      'double': 'double',
      'decimal': 'decimal',
      'timestamp_ltz': 'timestamp',
    };

    // 处理带参数的类型，如 varchar(100), decimal(10,2)
    const baseType = dbType.toLowerCase().split('(')[0];
    return typeMap[baseType] || baseType;
  };

  // 创建初始数据并打开创建对话框
  const proceedToCreateModal = (schemaContent: string) => {
    if (!selectedTable) return;

    // 确保表ID格式正确，转为小写并用下划线连接
    const tableId = generateTableId(selectedTable.database, selectedTable.tableName);
    
    // 创建初始数据传给CreateTableModal
    const initialData = {
      id: tableId,
      schema: schemaContent,
      status: TableStatus.OFFLINE,
      isPartitioned: false, // 默认非分区表，用户可在创建对话框中修改
      partitionExpr: "trunc(create_time, 'year')", // 默认按年分区表达式
      tags: '从业务系统导入',
      description: `从业务表${selectedTable.database}.${selectedTable.tableName}导入的Hudi表`,
      sourceDb: selectedTable.database.toLowerCase(),
      sourceTable: selectedTable.tableName.toLowerCase(),
      dbType: 'tdsql', // 默认数据库类型
      hoodieConfig: '{}', // 空配置，在CreateTableModal中会自动设置默认值
    };

    console.log('传递给CreateTableModal的参数:', initialData);
    setCreateModalInitialData(initialData);
    setCreateModalVisible(true);
  };

  // 下一步 - 获取详细表结构并打开编辑对话框
  const handleNext = () => {
    if (!selectedServer || !selectedDatabase || !selectedTable) {
      message.warning('请选择数据库服务器、数据库和业务表');
      return;
    }

    // 尝试获取详细的表结构信息
    fetchTableSchema(selectedServer, selectedDatabase, selectedTable.tableName);
  };

  // 处理CreateModal成功
  const handleCreateModalSuccess = () => {
    setCreateModalVisible(false);
    setCreateModalInitialData(null);
    message.success('表创建成功');
    onSuccess();
  };

  // 处理CreateModal取消
  const handleCreateModalCancel = () => {
    setCreateModalVisible(false);
    setCreateModalInitialData(null);
  };

  // 处理取消
  const handleCancel = () => {
    resetState();
    onCancel();
  };



  return (
    <>
      <Modal
        title={
          <Space>
            <ImportOutlined />
            从业务系统导入表
          </Space>
        }
        open={visible}
        onCancel={handleCancel}
        className="import-business-table-modal"
        footer={
          <Space size="middle">
            <Button 
              type="primary" 
              onClick={handleNext} 
              disabled={!selectedTable}
              loading={tableSchemaLoading}
            >
              {tableSchemaLoading ? '获取表结构中...' : '下一步'}
            </Button>
            <Button onClick={handleCancel}>
              取消
            </Button>
          </Space>
        }
        width={1000}
        destroyOnClose
      >
        {/* 表单严格左对齐布局 */}
        <Form
          form={form}
          layout="vertical"
          colon={true}
          style={{ textAlign: 'left' }}
        >
          {/* 数据库服务器选择 */}
          <Form.Item 
            label="数据库服务器" 
            name="server"
            required
            style={{ 
              marginBottom: '24px',
              textAlign: 'left'
            }}
            labelAlign="left"
          >
            <Select
              placeholder="请选择数据库服务器"
              loading={dbServersLoading}
              value={selectedServer}
              onChange={handleServerChange}
              style={{ 
                width: '100%',
                textAlign: 'left'
              }}
              dropdownAlign={{
                points: ['tl', 'bl'],
                offset: [0, 4],
                overflow: {
                  adjustX: true,
                  adjustY: true,
                },
              }}
            >
              {dbServers?.data?.map((server: DbServerDTO) => (
                <Option key={server.id} value={server.id}>
                  <Space>
                    <DatabaseOutlined />
                    {server.name} ({server.sourceType})
                  </Space>
                </Option>
              ))}
            </Select>
          </Form.Item>

          {/* 数据库选择 */}
          <Form.Item 
            label="数据库" 
            name="database"
            required
            style={{ 
              marginBottom: '24px',
              textAlign: 'left'
            }}
            labelAlign="left"
          >
            <Select
              placeholder="请选择数据库"
              loading={databasesLoading}
              value={selectedDatabase}
              onChange={handleDatabaseChange}
              disabled={!selectedServer}
              style={{ 
                width: '100%',
                textAlign: 'left'
              }}
              dropdownAlign={{
                points: ['tl', 'bl'],
                offset: [0, 4],
                overflow: {
                  adjustX: true,
                  adjustY: true,
                },
              }}
            >
              {databases?.map((db: string) => (
                <Option key={db} value={db}>
                  {db}
                </Option>
              ))}
            </Select>
          </Form.Item>

          {/* 业务表列表选择 */}
          <Form.Item 
            label="业务表列表" 
            name="businessTable"
            required
            style={{ 
              marginBottom: '24px',
              textAlign: 'left'
            }}
            labelAlign="left"
          >
            <Select
              placeholder="请选择业务表"
              loading={businessTablesLoading}
              value={selectedTable?.tableName}
              onChange={(tableName) => {
                const table = businessTables.find(t => t.tableName === tableName);
                if (table) {
                  handleTableSelect(table);
                }
              }}
              disabled={!selectedDatabase || businessTables.length === 0}
              showSearch
              optionLabelProp="label"
              filterOption={(input, option) => {
                const table = businessTables.find(t => t.tableName === option?.value);
                if (!table) return false;
                
                const searchText = input.toLowerCase();
                return Boolean(
                  table.tableName.toLowerCase().includes(searchText) ||
                  (table.tableComment && table.tableComment.toLowerCase().includes(searchText)) ||
                  (table.engine && table.engine.toLowerCase().includes(searchText))
                );
              }}
              style={{ 
                width: '100%',
                textAlign: 'left'
              }}
              dropdownAlign={{
                points: ['tl', 'bl'],
                offset: [0, 4],
                overflow: {
                  adjustX: true,
                  adjustY: true,
                },
              }}
            >
              {businessTables.map((table: BusinessTableInfo) => (
                <Option key={table.tableName} value={table.tableName} label={table.tableName}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <div>
                      <Text strong>{table.tableName}</Text>
                      {table.tableComment && (
                        <div style={{ fontSize: '12px', color: '#666', marginTop: '2px' }}>
                          {table.tableComment}
                        </div>
                      )}
                    </div>
                    {table.engine && (
                      <Tag style={{ marginLeft: '8px', fontSize: '12px' }}>
                        {table.engine}
                      </Tag>
                    )}
                  </div>
                </Option>
              ))}
            </Select>
          </Form.Item>
        </Form>
        
      </Modal>

      <CreateTableModal
        visible={createModalVisible}
        initialData={createModalInitialData}
        onCancel={handleCreateModalCancel}
        onSuccess={handleCreateModalSuccess}
      />
    </>
  );
};

export default ImportBusinessTableModal; 