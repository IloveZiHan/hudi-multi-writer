import React, { useState, useEffect } from 'react';
import {
  Modal,
  Form,
  Input,
  Select,
  Switch,
  Button,
  Row,
  Col,
  message,
  Space,
  Divider,
  Table,
  Tag,
  Badge,
  Typography,
  Card,
  Popconfirm,
  Tabs,
  Radio,
} from 'antd';
import { EditOutlined, DatabaseOutlined, PlusOutlined, DeleteOutlined } from '@ant-design/icons';
import { useRequest } from 'ahooks';
import tableApiService from '@services/tableApi';
import { UpdateTableRequest, TableStatus, MetaTableDTO } from '@types/api';

const { Option } = Select;
const { TextArea } = Input;
const { Text } = Typography;
const { TabPane } = Tabs;
const { confirm } = Modal;

interface EditTableModalProps {
  visible: boolean;
  record: MetaTableDTO | null;
  onCancel: () => void;
  onSuccess: () => void;
}

/**
 * 编辑表模态框组件
 */
const EditTableModal: React.FC<EditTableModalProps> = ({
  visible,
  record,
  onCancel,
  onSuccess,
}) => {
  const [form] = Form.useForm();
  const [isPartitioned, setIsPartitioned] = useState(false);
  const [schemaEditMode, setSchemaEditMode] = useState<'visual' | 'text'>('visual'); // 表结构编辑模式
  const [schemaValue, setSchemaValue] = useState(''); // 表结构值
  const [schemaFields, setSchemaFields] = useState<any[]>([]);
  const [editingKey, setEditingKey] = useState<string>('');
  const [initialValues, setInitialValues] = useState<any>({}); // 用于存储初始值

  // 解析表结构 JSON
  const parseTableSchema = (schema: string) => {
    try {
      const parsed = JSON.parse(schema);
      if (parsed.type === 'struct' && parsed.fields) {
        return parsed.fields.map((field: any, index: number) => ({
          key: index.toString(),
          name: field.name,
          type: field.type,
          nullable: field.nullable,
          comment: field.metadata?.comment || '',
        }));
      }
      return [];
    } catch (error) {
      console.error('解析表结构失败:', error);
      return [];
    }
  };

  // 将字段数据转换为Schema JSON
  const fieldsToSchema = (fields: any[]) => {
    const schemaFields = fields.map((field) => ({
      name: field.name,
      type: field.type,
      nullable: field.nullable,
      metadata: {
        comment: field.comment || '',
      },
    }));
    
    return JSON.stringify({
      type: 'struct',
      fields: schemaFields,
    }, null, 2);
  };

  // 更新Schema字段
  const updateSchemaFields = (fields: any[]) => {
    setSchemaFields(fields);
    const schemaJson = fieldsToSchema(fields);
    setSchemaValue(schemaJson);
    form.setFieldsValue({ schema: schemaJson });
  };

  // 添加新字段
  const handleAddField = () => {
    const newField = {
      key: Date.now().toString(),
      name: '',
      type: 'string',
      nullable: true,
      comment: '',
    };
    const newFields = [...schemaFields, newField];
    updateSchemaFields(newFields);
    setEditingKey(newField.key);
  };

  // 删除字段
  const handleDeleteField = (key: string) => {
    const newFields = schemaFields.filter(field => field.key !== key);
    updateSchemaFields(newFields);
  };

  // 保存编辑
  const handleSave = (key: string) => {
    setEditingKey('');
  };

  // 取消编辑
  const handleCancelEdit = () => {
    setEditingKey('');
  };

  // 是否正在编辑
  const isEditing = (record: any) => record.key === editingKey;

  // 表结构表格列定义
  const schemaColumns = [
    {
      title: '字段名',
      dataIndex: 'name',
      key: 'name',
      width: 150,
      render: (text: string, record: any) => {
        const editing = isEditing(record);
        return editing ? (
          <Input
            value={text}
            onChange={(e) => {
              const newFields = schemaFields.map(field => 
                field.key === record.key ? { ...field, name: e.target.value } : field
              );
              updateSchemaFields(newFields);
            }}
            onPressEnter={() => handleSave(record.key)}
            onBlur={() => handleSave(record.key)}
            placeholder="输入字段名"
          />
        ) : (
          <Text strong>{text || '(未命名字段)'}</Text>
        );
      },
    },
    {
      title: '数据类型',
      dataIndex: 'type',
      key: 'type',
      width: 120,
      render: (text: string, record: any) => {
        const editing = isEditing(record);
        return editing ? (
          <Select
            value={text}
            onChange={(value) => {
              const newFields = schemaFields.map(field => 
                field.key === record.key ? { ...field, type: value } : field
              );
              updateSchemaFields(newFields);
            }}
            style={{ width: '100%' }}
          >
            <Option value="string">string</Option>
            <Option value="int">int</Option>
            <Option value="bigint">bigint</Option>
            <Option value="double">double</Option>
            <Option value="boolean">boolean</Option>
            <Option value="timestamp">timestamp</Option>
            <Option value="date">date</Option>
            <Option value="decimal">decimal</Option>
          </Select>
        ) : (
          <Tag color="blue">{text}</Tag>
        );
      },
    },
    {
      title: '是否可为空',
      dataIndex: 'nullable',
      key: 'nullable',
      width: 120,
      render: (nullable: boolean, record: any) => {
        const editing = isEditing(record);
        return editing ? (
          <Switch
            checked={nullable}
            onChange={(checked) => {
              const newFields = schemaFields.map(field => 
                field.key === record.key ? { ...field, nullable: checked } : field
              );
              updateSchemaFields(newFields);
            }}
            checkedChildren="可为空"
            unCheckedChildren="不可为空"
          />
        ) : (
          <Badge
            status={nullable ? 'warning' : 'success'}
            text={nullable ? '可为空' : '不可为空'}
          />
        );
      },
    },
    {
      title: '注释',
      dataIndex: 'comment',
      key: 'comment',
      render: (text: string, record: any) => {
        const editing = isEditing(record);
        return editing ? (
          <Input
            value={text}
            onChange={(e) => {
              const newFields = schemaFields.map(field => 
                field.key === record.key ? { ...field, comment: e.target.value } : field
              );
              updateSchemaFields(newFields);
            }}
            onPressEnter={() => handleSave(record.key)}
            onBlur={() => handleSave(record.key)}
            placeholder="输入注释"
          />
        ) : (
          !text ? (
            <Text type="secondary">无注释</Text>
          ) : (
            <Text>{text}</Text>
          )
        );
      },
    },
    {
      title: '操作',
      key: 'action',
      width: 120,
      render: (text: string, record: any) => {
        const editing = isEditing(record);
        return editing ? (
          <Space>
            <Button
              type="link"
              onClick={() => handleSave(record.key)}
              size="small"
            >
              保存
            </Button>
            <Button
              type="link"
              onClick={handleCancelEdit}
              size="small"
            >
              取消
            </Button>
          </Space>
        ) : (
          <Space>
            <Button
              type="link"
              icon={<EditOutlined />}
              onClick={() => setEditingKey(record.key)}
              size="small"
            >
              编辑
            </Button>
            <Popconfirm
              title="确定要删除这个字段吗？"
              onConfirm={() => handleDeleteField(record.key)}
              okText="确定"
              cancelText="取消"
            >
              <Button
                type="link"
                icon={<DeleteOutlined />}
                danger
                size="small"
              >
                删除
              </Button>
            </Popconfirm>
          </Space>
        );
      },
    },
  ];

  // 更新表请求
  const { loading, run: updateTable } = useRequest(
    (id: string, data: UpdateTableRequest) => tableApiService.updateTable(id, data),
    {
      manual: true,
      onSuccess: () => {
        message.success('更新表成功');
        onSuccess();
      },
      onError: (error) => {
        message.error(`更新表失败: ${error.message}`);
      },
    }
  );

  // 生成格式化的JSON Schema
  const getFormattedSchema = () => {
    try {
      if (schemaEditMode === 'visual') {
        return fieldsToSchema(schemaFields);
      } else {
        if (schemaValue) {
          return JSON.stringify(JSON.parse(schemaValue), null, 2);
        }
        return '';
      }
    } catch (error) {
      return schemaValue;
    }
  };

  // 处理文本编辑模式下的schema变化
  const handleSchemaTextChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    const value = e.target.value;
    setSchemaValue(value);
    form.setFieldsValue({ schema: value });
    
    // 尝试解析并更新可视化字段
    try {
      const fields = parseTableSchema(value);
      setSchemaFields(fields);
    } catch (error) {
      // 解析失败时不更新可视化字段
    }
  };

  // 处理编辑模式切换
  const handleEditModeChange = (mode: 'visual' | 'text') => {
    setSchemaEditMode(mode);
    if (mode === 'visual') {
      // 切换到可视化模式时，尝试解析现有的schema
      try {
        if (schemaValue) {
          const fields = parseTableSchema(schemaValue);
          setSchemaFields(fields);
        }
      } catch (error) {
        // 解析失败时保持现有字段
      }
    }
  };

  // 修改 useEffect 来保存初始值
  useEffect(() => {
    if (visible && record) {
      const values = {
        status: record.status,
        sourceDb: record.sourceDb,
        sourceTable: record.sourceTable,
        isPartitioned: record.isPartitioned,
        partitionExpr: record.partitionExpr,
        tags: record.tags,
        description: record.description,
        schema: record.schema,
      };
      
      form.setFieldsValue(values);
      setInitialValues(values); // 保存初始值
      setIsPartitioned(record.isPartitioned || false);
      setSchemaValue(record.schema || '');
      
      // 解析表结构
      const fields = parseTableSchema(record.schema || '');
      setSchemaFields(fields);
    }
  }, [visible, record, form]);

  // 处理表单提交
  const handleSubmit = async () => {
    if (!record) return;
    
    try {
      const values = await form.validateFields();
      await updateTable(record.id, values);
    } catch (error) {
      // 表单验证失败
    }
  };

  // 处理取消
  const handleCancel = () => {
    // 检查表单是否被修改过
    const currentValues = form.getFieldsValue();
    const isFormTouched = form.isFieldsTouched() || 
                          JSON.stringify(currentValues) !== JSON.stringify(initialValues) ||
                          schemaFields.length !== parseTableSchema(record?.schema || '').length;
    
    if (isFormTouched) {
      confirm({
        title: '确认关闭',
        content: '您已经修改了表单内容，确定要关闭吗？修改的内容将丢失。',
        okText: '确认',
        cancelText: '取消',
        onOk: () => {
          form.resetFields();
          setIsPartitioned(false);
          setSchemaEditMode('visual');
          setSchemaValue('');
          setSchemaFields([]);
          setEditingKey('');
          setInitialValues({});
          onCancel();
        },
      });
    } else {
      form.resetFields();
      setIsPartitioned(false);
      setSchemaEditMode('visual');
      setSchemaValue('');
      setSchemaFields([]);
      setEditingKey('');
      setInitialValues({});
      onCancel();
    }
  };

  // 处理模态框的 X 按钮和遮罩点击
  const handleModalCancel = () => {
    handleCancel();
  };

  return (
    <Modal
      title={
        <Space>
          <EditOutlined />
          编辑表 - {record?.id}
        </Space>
      }
      open={visible}
      onCancel={handleModalCancel}
      footer={[
        <Button key="cancel" onClick={handleCancel}>
          取消
        </Button>,
        <Button key="submit" type="primary" loading={loading} onClick={handleSubmit}>
          更新
        </Button>,
      ]}
      width={1000}
      destroyOnClose
    >
      <Form
        form={form}
        layout="vertical"
      >
        <Divider orientation="left">
          <DatabaseOutlined /> 基本信息
        </Divider>
        
        <Row gutter={16}>
          <Col span={12}>
            <Form.Item label="表ID">
              <Input value={record?.id} disabled />
            </Form.Item>
          </Col>
          <Col span={12}>
            <Form.Item
              label="状态"
              name="status"
              rules={[{ required: true, message: '请选择状态' }]}
            >
              <Select placeholder="请选择状态">
                <Option value={TableStatus.OFFLINE}>未上线</Option>
                <Option value={TableStatus.ONLINE}>已上线</Option>
              </Select>
            </Form.Item>
          </Col>
        </Row>

        <Form.Item
          label={
            <Space>
              <span>表结构</span>
            </Space>
          }
        >
          <Card size="small">
            <div style={{ marginBottom: 16 }}>
              <Radio.Group
                value={schemaEditMode}
                onChange={(e) => handleEditModeChange(e.target.value)}
              >
                <Radio.Button value="visual">可视化编辑</Radio.Button>
                <Radio.Button value="text">文本编辑</Radio.Button>
              </Radio.Group>
            </div>
            
            <Tabs defaultActiveKey="editor" type="card">
              <TabPane tab="编辑器" key="editor">
                {/* 隐藏的schema字段，用于存储JSON数据 */}
                <Form.Item
                  name="schema"
                  rules={[{ required: true, message: '请输入表结构' }]}
                  hidden
                >
                  <Input />
                </Form.Item>

                {schemaEditMode === 'visual' ? (
                  <div>
                    <div style={{ marginBottom: 16 }}>
                      <Button
                        type="dashed"
                        onClick={handleAddField}
                        icon={<PlusOutlined />}
                        style={{ width: '100%' }}
                      >
                        添加字段
                      </Button>
                    </div>
                    <Table
                      columns={schemaColumns}
                      dataSource={schemaFields}
                      pagination={false}
                      size="small"
                      bordered
                      scroll={{ x: 800, y: 300 }}
                      locale={{
                        emptyText: '暂无字段信息，请点击"添加字段"添加表结构',
                      }}
                    />
                  </div>
                ) : (
                  <TextArea
                    rows={8}
                    placeholder="请输入表结构JSON或DDL语句"
                    style={{ fontFamily: 'monospace' }}
                    value={schemaValue}
                    onChange={handleSchemaTextChange}
                  />
                )}
              </TabPane>
              
              <TabPane tab="JSON Schema预览" key="preview">
                <div style={{ 
                  border: '1px solid #d9d9d9', 
                  borderRadius: '6px',
                  padding: '16px',
                  minHeight: '300px',
                  background: '#f6f6f6'
                }}>
                  <pre style={{ 
                    margin: 0, 
                    fontFamily: 'monospace',
                    fontSize: '12px',
                    whiteSpace: 'pre-wrap',
                    wordBreak: 'break-word'
                  }}>
                    {getFormattedSchema() || '// 请在编辑器中输入表结构或添加字段'}
                  </pre>
                </div>
              </TabPane>
            </Tabs>
          </Card>
        </Form.Item>

        <Divider orientation="left">数据源配置</Divider>

        <Row gutter={16}>
          <Col span={12}>
            <Form.Item
              label="源数据库"
              name="sourceDb"
              rules={[{ required: true, message: '请输入源数据库' }]}
            >
              <Input placeholder="请输入源数据库名称" />
            </Form.Item>
          </Col>
          <Col span={12}>
            <Form.Item
              label="源表名"
              name="sourceTable"
              rules={[{ required: true, message: '请输入源表名' }]}
            >
              <Input placeholder="请输入源表名称" />
            </Form.Item>
          </Col>
        </Row>

        <Divider orientation="left">分区配置</Divider>

        <Row gutter={16}>
          <Col span={12}>
            <Form.Item
              label="是否分区表"
              name="isPartitioned"
              valuePropName="checked"
            >
              <Switch
                onChange={(checked) => setIsPartitioned(checked)}
                checkedChildren="是"
                unCheckedChildren="否"
              />
            </Form.Item>
          </Col>
          <Col span={12}>
            {isPartitioned && (
              <Form.Item
                label="分区表达式"
                name="partitionExpr"
                rules={[{ required: isPartitioned, message: '请输入分区表达式' }]}
              >
                <Input placeholder="例如: year, month, day" />
              </Form.Item>
            )}
          </Col>
        </Row>

        <Divider orientation="left">其他信息</Divider>

        <Form.Item
          label="标签"
          name="tags"
        >
          <Input placeholder="请输入标签，多个标签用逗号分隔" />
        </Form.Item>

        <Form.Item
          label="描述"
          name="description"
        >
          <TextArea
            rows={3}
            placeholder="请输入表描述信息"
            maxLength={500}
            showCount
          />
        </Form.Item>

        <Divider orientation="left">元数据信息</Divider>

        <Row gutter={16}>
          <Col span={12}>
            <Form.Item label="创建时间">
              <Input value={record?.createdTime} disabled />
            </Form.Item>
          </Col>
          <Col span={12}>
            <Form.Item label="更新时间">
              <Input value={record?.updatedTime} disabled />
            </Form.Item>
          </Col>
        </Row>
      </Form>
    </Modal>
  );
};

export default EditTableModal; 