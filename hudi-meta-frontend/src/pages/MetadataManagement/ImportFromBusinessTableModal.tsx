import React, { useState, useEffect } from 'react';
import {
  Modal,
  Form,
  Select,
  Button,
  Space,
  message,
  Table,
  Typography,
  Card,
  Row,
  Col,
  Tag,
  Badge,
  Tooltip,
  Alert,
  Divider,
} from 'antd';
import {
  DatabaseOutlined,
  TableOutlined,
  EyeOutlined,
  ImportOutlined,
  LoadingOutlined,
} from '@ant-design/icons';
import { useRequest } from 'ahooks';
import type { ColumnsType } from 'antd/es/table';
import dbServerApiService, { DbServerDTO, BusinessTableInfo, TableSchemaInfo } from '@services/dbServerApi';
import hoodieConfigApiService from '@services/hoodieConfigApi';

const { Option } = Select;
const { Text, Title } = Typography;

interface ImportFromBusinessTableModalProps {
  visible: boolean;
  onCancel: () => void;
  onImport: (importData: ImportTableData) => void;
}

/**
 * 导入表数据接口
 */
export interface ImportTableData {
  tableId: string;
  schema: string;
  dbType: string;
  sourceDb: string;
  sourceTable: string;
  hoodieConfig: string; // 新增：Hoodie配置
  fields: Array<{
    name: string;
    type: string;
    nullable: boolean;
    comment: string;
  }>;
}

/**
 * 从业务表导入对话框组件
 */
const ImportFromBusinessTableModal: React.FC<ImportFromBusinessTableModalProps> = ({
  visible,
  onCancel,
  onImport,
}) => {
  const [form] = Form.useForm();
  const [selectedServerId, setSelectedServerId] = useState<number | undefined>();
  const [selectedDatabase, setSelectedDatabase] = useState<string | undefined>();
  const [selectedTable, setSelectedTable] = useState<BusinessTableInfo | undefined>();
  const [tableSchema, setTableSchema] = useState<TableSchemaInfo | undefined>();

  // 获取数据库服务器列表
  const { data: dbServers, loading: serversLoading } = useRequest(
    () => dbServerApiService.getAllDbServers(0, 100),
    {
      onError: (error: any) => {
        message.error(`获取数据源列表失败: ${error?.message || '未知错误'}`);
      },
    }
  );

  // 获取数据库列表
  const { data: databases, loading: databasesLoading, run: fetchDatabases } = useRequest(
    (serverId: number) => dbServerApiService.getDatabases(serverId),
    {
      manual: true,
      onError: (error: any) => {
        message.error(`获取数据库列表失败: ${error?.message || '未知错误'}`);
      },
    }
  );

  // 获取业务表列表
  const { data: businessTables, loading: tablesLoading, run: fetchBusinessTables } = useRequest(
    (serverId: number, database?: string) => dbServerApiService.getBusinessTables(serverId, database),
    {
      manual: true,
      onError: (error: any) => {
        console.error('获取业务表列表失败:', error);
        message.error(`获取业务表列表失败: ${error?.message || '未知错误'}`);
      },
    }
  );

  // 获取表结构
  const { loading: schemaLoading, run: fetchTableSchema } = useRequest(
    (serverId: number, database: string, tableName: string) => 
      dbServerApiService.getTableSchema(serverId, database, tableName),
    {
      manual: true,
      onSuccess: (data) => {
        setTableSchema(data);
      },
      onError: (error: any) => {
        console.error('获取表结构失败:', error);
        message.error(`获取表结构失败: ${error?.message || '未知错误'}`);
      },
    }
  );

  // 获取默认Hoodie配置
  const { loading: loadingDefaultConfig, runAsync: getDefaultConfig } = useRequest(
    () => hoodieConfigApiService.getDefaultConfig(),
    {
      manual: true,
      onError: (error: any) => {
        console.error('获取默认Hoodie配置失败:', error);
        message.error(`获取默认Hoodie配置失败: ${error?.message || '未知错误'}`);
      },
    }
  );

  // 处理数据源选择
  const handleServerChange = (serverId: number) => {
    console.log('选择数据源:', serverId);
    setSelectedServerId(serverId);
    setSelectedDatabase(undefined);
    setSelectedTable(undefined);
    setTableSchema(undefined);
    form.setFieldsValue({ database: undefined, table: undefined });
    console.log('开始获取数据库列表...');
    fetchDatabases(serverId);
  };

  // 处理数据库选择
  const handleDatabaseChange = (database: string) => {
    console.log('选择数据库:', database, '服务器ID:', selectedServerId);
    setSelectedDatabase(database);
    setSelectedTable(undefined);
    setTableSchema(undefined);
    form.setFieldsValue({ table: undefined });
    if (selectedServerId) {
      console.log('开始获取业务表列表...');
      fetchBusinessTables(selectedServerId, database);
    } else {
      console.error('未选择服务器ID，无法获取业务表列表');
    }
  };

  // 处理表选择
  const handleTableChange = (tableName: string) => {
    const table = businessTables?.find(t => t.tableName === tableName);
    setSelectedTable(table);
    setTableSchema(undefined);
    
    if (selectedServerId && selectedDatabase && table) {
      fetchTableSchema(selectedServerId, selectedDatabase, table.tableName);
    }
  };

  // 数据类型映射
  const mapDataType = (mysqlType: string): string => {
    const type = mysqlType.toLowerCase();
    if (type.includes('varchar') || type.includes('char') || type.includes('text')) {
      return 'string';
    }
    if (type.includes('int') || type.includes('tinyint') || type.includes('smallint')) {
      return 'int';
    }
    if (type.includes('bigint')) {
      return 'bigint';
    }
    if (type.includes('decimal') || type.includes('numeric')) {
      return 'decimal';
    }
    if (type.includes('float') || type.includes('double')) {
      return 'double';
    }
    if (type.includes('datetime') || type.includes('timestamp')) {
      return 'timestamp';
    }
    if (type.includes('date')) {
      return 'date';
    }
    if (type.includes('boolean') || type.includes('bit')) {
      return 'boolean';
    }
    return 'string'; // 默认类型
  };

  // 处理导入
  const handleImport = () => {
    if (!selectedTable || !tableSchema || !selectedServerId) {
      message.warning('请选择完整的数据源、数据库和表');
      return;
    }

    const selectedServer = dbServers?.data.find(s => s.id === selectedServerId);
    if (!selectedServer) {
      message.error('无法找到选中的数据源');
      return;
    }

    // 先获取默认Hoodie配置，等待完成后再继续
    getDefaultConfig()
      .then((defaultConfig: string) => {
        console.log('获取到的默认配置:', defaultConfig);
        
        // 解析默认配置
        let hoodieConfig = {};
        try {
          hoodieConfig = JSON.parse(defaultConfig);
          console.log('解析后的默认配置:', hoodieConfig);
        } catch (error) {
          console.error('解析默认Hoodie配置失败:', error);
          message.error('解析默认Hoodie配置失败，使用基础配置');
          hoodieConfig = {};
        }

        // 设置主键字段
        if (tableSchema.primaryKeys && tableSchema.primaryKeys.length > 0) {
          (hoodieConfig as any)['hoodie.table.recordkey.fields'] = tableSchema.primaryKeys.join(',').toLowerCase();
          console.log('设置主键字段后的配置:', hoodieConfig);
        }

        // 构建导入数据
        const importData: ImportTableData = {
          tableId: `${selectedDatabase}_${selectedTable.tableName}`,
          schema: JSON.stringify({
            type: 'struct',
            fields: tableSchema.fields.map(field => ({
              name: field.fieldName,
              type: mapDataType(field.fieldType),
              nullable: field.isNullable,
              metadata: {
                comment: field.comment || '',
              },
            })),
          }, null, 2),
          dbType: selectedServer.sourceType,
          sourceDb: selectedDatabase!,
          sourceTable: selectedTable.tableName,
          hoodieConfig: JSON.stringify(hoodieConfig, null, 2),
          fields: tableSchema.fields.map(field => ({
            name: field.fieldName,
            type: mapDataType(field.fieldType),
            nullable: field.isNullable,
            comment: field.comment || '',
          })),
        };

        console.log('最终传递的导入数据:', importData);
        console.log('最终传递的 hoodieConfig:', importData.hoodieConfig);

        // 配置获取完成后，执行导入
        onImport(importData);
        handleModalClose();
      })
      .catch((error: any) => {
        console.error('获取默认配置失败:', error);
        message.error('获取默认配置失败，请重试');
      });
  };

  // 关闭对话框
  const handleModalClose = () => {
    form.resetFields();
    setSelectedServerId(undefined);
    setSelectedDatabase(undefined);
    setSelectedTable(undefined);
    setTableSchema(undefined);
    onCancel();
  };

  // 字段表格列定义
  const fieldColumns: ColumnsType<any> = [
    {
      title: '字段名',
      dataIndex: 'fieldName',
      key: 'fieldName',
      width: 150,
      render: (text: string, record: any) => (
        <Space>
          <Text strong>{text}</Text>
          {record.isPrimaryKey && <Tag color="gold">主键</Tag>}
        </Space>
      ),
    },
    {
      title: '数据类型',
      dataIndex: 'fieldType',
      key: 'fieldType',
      width: 120,
      render: (type: string) => <Tag color="blue">{type}</Tag>,
    },
    {
      title: 'Hudi类型',
      key: 'hudiType',
      width: 120,
      render: (_, record: any) => (
        <Tag color="green">{mapDataType(record.fieldType)}</Tag>
      ),
    },
    {
      title: '可为空',
      dataIndex: 'isNullable',
      key: 'isNullable',
      width: 80,
      render: (nullable: boolean) => (
        <Badge
          status={nullable ? 'warning' : 'success'}
          text={nullable ? '是' : '否'}
        />
      ),
    },
    {
      title: '默认值',
      dataIndex: 'defaultValue',
      key: 'defaultValue',
      width: 100,
      render: (value: string) => (
        <Text type="secondary">{value || '-'}</Text>
      ),
    },
    {
      title: '注释',
      dataIndex: 'comment',
      key: 'comment',
      render: (comment: string) => (
        <Tooltip title={comment}>
          <Text ellipsis style={{ maxWidth: 200 }}>
            {comment || '-'}
          </Text>
        </Tooltip>
      ),
    },
  ];

  return (
    <Modal
      title={
        <Space>
          <ImportOutlined />
          从业务表导入
        </Space>
      }
      visible={visible}
      onCancel={handleModalClose}
      width={1000}
      footer={[
        <Button key="cancel" onClick={handleModalClose}>
          取消
        </Button>,
        <Button
          key="import"
          type="primary"
          icon={<ImportOutlined />}
          onClick={handleImport}
          disabled={!tableSchema || databasesLoading || tablesLoading || schemaLoading || loadingDefaultConfig}
          loading={schemaLoading || loadingDefaultConfig}
        >
          {schemaLoading ? '正在获取表结构...' : 
           loadingDefaultConfig ? '正在获取默认配置...' : '导入并创建表'}
        </Button>,
      ]}
    >
      <Alert
        message="导入说明"
        description="选择业务数据源和表后，系统将自动获取表结构并创建对应的Hudi表。数据类型会自动映射为Hudi支持的类型。"
        type="info"
        showIcon
        style={{ marginBottom: 24 }}
      />

      <Form form={form} layout="vertical">
        <Row gutter={16}>
          <Col span={8}>
            <Form.Item
              label="选择数据源"
              name="dataSource"
              rules={[{ required: true, message: '请选择数据源' }]}
            >
              <Select
                placeholder={serversLoading ? "正在加载数据源列表..." : "请选择数据源"}
                loading={serversLoading}
                onChange={handleServerChange}
                disabled={serversLoading}
                showSearch
                filterOption={(input, option) =>
                  (option?.children?.toString().toLowerCase().includes(input.toLowerCase()) ?? false)
                }
              >
                {(dbServers?.data || []).map((server: DbServerDTO) => (
                  <Option key={server.id} value={server.id}>
                    <Space>
                      <DatabaseOutlined />
                      {server.name} ({server.sourceType.toUpperCase()})
                    </Space>
                  </Option>
                ))}
              </Select>
            </Form.Item>
          </Col>
          <Col span={8}>
            <Form.Item
              label="选择数据库"
              name="database"
              rules={[{ required: true, message: '请选择数据库' }]}
            >
              <Select
                placeholder={databasesLoading ? "正在加载数据库列表..." : "请选择数据库"}
                loading={databasesLoading}
                onChange={handleDatabaseChange}
                disabled={!selectedServerId || databasesLoading}
                showSearch
                filterOption={(input, option) =>
                  (option?.children?.toString().toLowerCase().includes(input.toLowerCase()) ?? false)
                }
              >
                {databases?.map((db: string) => (
                  <Option key={db} value={db}>
                    {db}
                  </Option>
                ))}
              </Select>
            </Form.Item>
          </Col>
          <Col span={8}>
            <Form.Item
              label="选择业务表"
              name="table"
              rules={[{ required: true, message: '请选择业务表' }]}
            >
              <Select
                placeholder={tablesLoading ? "正在加载业务表列表..." : 
                  !selectedDatabase ? "请先选择数据库" : 
                  businessTables && businessTables.length === 0 ? "该数据库下无业务表" : 
                  "请选择业务表"}
                loading={tablesLoading}
                onChange={handleTableChange}
                disabled={!selectedDatabase || tablesLoading}
                showSearch
                filterOption={(input, option) => {
                  const searchValue = input.toLowerCase();
                  const tableValue = option?.value?.toString().toLowerCase() || '';
                  // 查找对应的表信息
                  const tableInfo = businessTables?.find(t => t.tableName === option?.value);
                  const tableComment = tableInfo?.tableComment?.toLowerCase() || '';
                  
                  // 匹配表名或注释
                  return tableValue.includes(searchValue) || tableComment.includes(searchValue);
                }}
                notFoundContent={tablesLoading ? '加载中...' : '暂无数据'}
              >
                {businessTables?.map((table: BusinessTableInfo) => (
                  <Option key={table.tableName} value={table.tableName}>
                    <Space>
                      <TableOutlined />
                      {table.tableName}
                      {table.tableComment && (
                        <Text type="secondary">({table.tableComment})</Text>
                      )}
                    </Space>
                  </Option>
                ))}
              </Select>
            </Form.Item>
          </Col>
        </Row>
      </Form>

      {selectedTable && (
        <Card
          title={
            <Space>
              <TableOutlined />
              表信息：{selectedTable.tableName}
            </Space>
          }
          size="small"
          style={{ marginTop: 16 }}
        >
          <Row gutter={16}>
            <Col span={8}>
              <Text strong>表名：</Text>
              <Text>{selectedTable.tableName}</Text>
            </Col>
            <Col span={8}>
              <Text strong>数据库：</Text>
              <Text>{selectedDatabase}</Text>
            </Col>
            <Col span={8}>
              <Text strong>注释：</Text>
              <Text type="secondary">{selectedTable.tableComment || '无'}</Text>
            </Col>
          </Row>
        </Card>
      )}

      {schemaLoading && (
        <div style={{ textAlign: 'center', padding: '20px' }}>
          <LoadingOutlined style={{ fontSize: 24 }} />
          <div style={{ marginTop: 8 }}>正在获取表结构...</div>
        </div>
      )}

      {tableSchema && (
        <Card
          title={
            <Space>
              <EyeOutlined />
              表结构预览（共 {tableSchema.fields.length} 个字段）
            </Space>
          }
          size="small"
          style={{ marginTop: 16 }}
        >
          <Table
            columns={fieldColumns}
            dataSource={tableSchema.fields}
            rowKey="fieldName"
            pagination={false}
            scroll={{ y: 300 }}
            size="small"
            bordered
          />
          
          {tableSchema.primaryKeys.length > 0 && (
            <>
              <Divider />
              <div>
                <Text strong>主键字段：</Text>
                <Space wrap>
                  {tableSchema.primaryKeys.map(key => (
                    <Tag key={key} color="gold">{key}</Tag>
                  ))}
                </Space>
              </div>
            </>
          )}
        </Card>
      )}
    </Modal>
  );
};

export default ImportFromBusinessTableModal; 