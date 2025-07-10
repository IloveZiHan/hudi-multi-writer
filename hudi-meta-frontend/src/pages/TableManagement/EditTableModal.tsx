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
  AutoComplete,
} from 'antd';
import { EditOutlined, DatabaseOutlined, PlusOutlined, DeleteOutlined, SettingOutlined, InfoCircleOutlined, TableOutlined, FileTextOutlined } from '@ant-design/icons';
import { useRequest } from 'ahooks';
import tableApiService from '@services/tableApi';
import hoodieConfigApiService from '@services/hoodieConfigApi';
import { UpdateTableRequest, TableStatus, MetaTableDTO } from '../../types/api';

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
  const [activeTab, setActiveTab] = useState('basic'); // 当前活动Tab
  const [isPartitioned, setIsPartitioned] = useState(false);
  const [schemaEditMode, setSchemaEditMode] = useState<'visual' | 'text'>('visual'); // 表结构编辑模式
  const [schemaValue, setSchemaValue] = useState(''); // 表结构值
  const [schemaFields, setSchemaFields] = useState<any[]>([]);
  const [editingKey, setEditingKey] = useState<string>('');
  const [initialValues, setInitialValues] = useState<any>({}); // 用于存储初始值

  
  // Hoodie Config 相关状态
  const [hoodieConfigEditMode, setHoodieConfigEditMode] = useState<'visual' | 'text'>('visual'); // Hoodie配置编辑模式
  const [hoodieConfigValue, setHoodieConfigValue] = useState(''); // Hoodie配置值
  const [hoodieConfigFields, setHoodieConfigFields] = useState<any[]>([]); // 可视化编辑的配置数据
  const [editingHoodieConfigKey, setEditingHoodieConfigKey] = useState<string>(''); // 正在编辑的配置key

  // 分区表达式预设选项
  const partitionOptions = [
    { 
      label: '按年分区', 
      value: "trunc(create_time, 'year')",
      key: 'year'
    },
    { 
      label: '默认分区', 
      value: "'default'",
      key: 'default'
    },
    { 
      label: '按日分区', 
      value: 'substring(create_time, 1, 10)',
      key: 'day'
    }
  ];

  // 解析表结构 JSON
  const parseTableSchema = (schema: string) => {
    try {
      const parsed = JSON.parse(schema);
      if (parsed.type === 'struct' && parsed.fields) {
        return parsed.fields.map((field: any, index: number) => ({
          key: index.toString(),
          name: field.name ? field.name.toLowerCase() : '', // 转换为小写，处理空值
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

  // 解析Hoodie配置JSON
  const parseHoodieConfig = (config: string) => {
    try {
      const parsed = JSON.parse(config);
      if (typeof parsed === 'object' && parsed !== null) {
        return Object.entries(parsed).map(([key, value], index) => ({
          key: index.toString(),
          configKey: key,
          configValue: String(value),
        }));
      }
      return [];
    } catch (error) {
      console.error('解析Hoodie配置失败:', error);
      return [];
    }
  };

  // 将字段数据转换为Schema JSON
  const fieldsToSchema = (fields: any[]) => {
    const schemaFields = fields.map((field) => ({
      name: field.name ? field.name.toLowerCase() : '', // 转换为小写，处理空值
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

  // 将配置数据转换为Hoodie配置JSON
  const fieldsToHoodieConfig = (fields: any[]) => {
    const configObj = fields.reduce((acc, field) => {
      if (field.configKey) {
        acc[field.configKey] = field.configValue;
      }
      return acc;
    }, {} as Record<string, any>);
    
    return JSON.stringify(configObj, null, 2);
  };

  // 更新Schema字段
  const updateSchemaFields = (fields: any[]) => {
    setSchemaFields(fields);
    const schemaJson = fieldsToSchema(fields);
    setSchemaValue(schemaJson);
    form.setFieldsValue({ schema: schemaJson });
  };

  // 更新Hoodie配置字段
  const updateHoodieConfigFields = (fields: any[]) => {
    setHoodieConfigFields(fields);
    const configJson = fieldsToHoodieConfig(fields);
    setHoodieConfigValue(configJson);
    form.setFieldsValue({ hoodieConfig: configJson });
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

  // 添加新Hoodie配置
  const handleAddHoodieConfig = () => {
    const newConfig = {
      key: Date.now().toString(),
      configKey: '',
      configValue: '',
    };
    const newFields = [...hoodieConfigFields, newConfig];
    updateHoodieConfigFields(newFields);
    setEditingHoodieConfigKey(newConfig.key);
  };

  // 删除字段
  const handleDeleteField = (key: string) => {
    const newFields = schemaFields.filter(field => field.key !== key);
    updateSchemaFields(newFields);
  };

  // 删除Hoodie配置
  const handleDeleteHoodieConfig = (key: string) => {
    const newFields = hoodieConfigFields.filter(field => field.key !== key);
    updateHoodieConfigFields(newFields);
  };



  // 保存编辑
  const handleSave = (key: string) => {
    setEditingKey('');
  };

  // 保存Hoodie配置编辑
  const handleSaveHoodieConfig = (key: string) => {
    setEditingHoodieConfigKey('');
  };

  // 取消编辑
  const handleCancelEdit = () => {
    setEditingKey('');
  };

  // 取消Hoodie配置编辑
  const handleCancelHoodieConfigEdit = () => {
    setEditingHoodieConfigKey('');
  };

  // 是否正在编辑
  const isEditing = (record: any) => record.key === editingKey;

  // 是否正在编辑Hoodie配置
  const isEditingHoodieConfig = (record: any) => record.key === editingHoodieConfigKey;

  // 表结构表格列定义
  const schemaColumns = [
    {
      title: '字段名',
      dataIndex: 'name',
      key: 'name',
      width: '25%',
      render: (text: string, record: any) => {
        const editing = isEditing(record);
        return editing ? (
          <Input
            value={text}
            onChange={(e) => {
              const value = e.target.value.toLowerCase(); // 自动转换为小写
              const newFields = schemaFields.map(field => 
                field.key === record.key ? { ...field, name: value } : field
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
      width: '15%',
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
      width: '15%',
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
      width: '25%',
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
      width: '20%',
      fixed: 'right' as 'right',
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

  // Hoodie配置表格列定义
  const hoodieConfigColumns = [
    {
      title: '配置键',
      dataIndex: 'configKey',
      key: 'configKey',
      width: '40%',
      render: (text: string, record: any) => {
        const editing = isEditingHoodieConfig(record);
        return editing ? (
          <Input
            value={text}
            onChange={(e) => {
              const newFields = hoodieConfigFields.map(field => 
                field.key === record.key ? { ...field, configKey: e.target.value } : field
              );
              updateHoodieConfigFields(newFields);
            }}
            onPressEnter={() => handleSaveHoodieConfig(record.key)}
            onBlur={() => handleSaveHoodieConfig(record.key)}
            placeholder="输入配置键"
          />
        ) : (
          <Text strong>{text || '(未命名配置)'}</Text>
        );
      },
    },
    {
      title: '配置值',
      dataIndex: 'configValue',
      key: 'configValue',
      width: '40%',
      render: (text: string, record: any) => {
        const editing = isEditingHoodieConfig(record);
        return editing ? (
          <Input
            value={text}
            onChange={(e) => {
              const newFields = hoodieConfigFields.map(field => 
                field.key === record.key ? { ...field, configValue: e.target.value } : field
              );
              updateHoodieConfigFields(newFields);
            }}
            onPressEnter={() => handleSaveHoodieConfig(record.key)}
            onBlur={() => handleSaveHoodieConfig(record.key)}
            placeholder="输入配置值"
          />
        ) : (
          <Text>{text || '(无值)'}</Text>
        );
      },
    },
    {
      title: '操作',
      key: 'action',
      width: '20%',
      fixed: 'right' as 'right',
      render: (text: string, record: any) => {
        const editing = isEditingHoodieConfig(record);
        return editing ? (
          <Space>
            <Button
              type="link"
              onClick={() => handleSaveHoodieConfig(record.key)}
              size="small"
            >
              保存
            </Button>
            <Button
              type="link"
              onClick={handleCancelHoodieConfigEdit}
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
              onClick={() => setEditingHoodieConfigKey(record.key)}
              size="small"
            >
              编辑
            </Button>
            <Popconfirm
              title="确定要删除这个配置吗？"
              onConfirm={() => handleDeleteHoodieConfig(record.key)}
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

  // 获取主键字段
  const getPrimaryKeyFields = () => {
    const primaryKeyFields: string[] = [];
    
    // 从可视化编辑器的字段中获取主键
    if (schemaFields.length > 0) {
      schemaFields.forEach(field => {
        if (field.name && (
          field.name.toLowerCase() === 'id' ||
          field.name.toLowerCase().includes('key') ||
          field.name.toLowerCase().includes('primary')
        )) {
          primaryKeyFields.push(field.name.toLowerCase());
        }
      });
    }
    
    // 从schema文本中解析主键
    if (primaryKeyFields.length === 0 && schemaValue) {
      try {
        const parsed = JSON.parse(schemaValue);
        if (parsed.type === 'struct' && parsed.fields) {
          parsed.fields.forEach((field: any) => {
            if (field.name && (
              field.name.toLowerCase() === 'id' ||
              field.name.toLowerCase().includes('key') ||
              field.name.toLowerCase().includes('primary')
            )) {
              primaryKeyFields.push(field.name.toLowerCase());
            }
          });
        }
      } catch (error) {
        console.error('解析schema获取主键失败:', error);
      }
    }
    
    // 如果没有找到明确的主键字段，使用第一个字段作为主键
    if (primaryKeyFields.length === 0) {
      if (schemaFields.length > 0 && schemaFields[0].name) {
        primaryKeyFields.push(schemaFields[0].name.toLowerCase());
      } else if (schemaValue) {
        try {
          const parsed = JSON.parse(schemaValue);
          if (parsed.type === 'struct' && parsed.fields && parsed.fields.length > 0) {
            const firstField = parsed.fields[0];
            if (firstField.name) {
              primaryKeyFields.push(firstField.name.toLowerCase());
            }
          }
        } catch (error) {
          console.error('解析schema获取第一个字段失败:', error);
        }
      }
    }
    
    return primaryKeyFields;
  };

  // 获取默认Hoodie配置请求
  const { loading: loadingDefaultConfig, run: getDefaultConfig } = useRequest(
    () => hoodieConfigApiService.getDefaultConfig(),
    {
      manual: true,
      onSuccess: (data) => {
        try {
          // 解析默认配置
          const parsedConfig = JSON.parse(data);
          
          // 获取主键字段
          const primaryKeyFields = getPrimaryKeyFields();
          
          // 设置主键字段到配置中
          if (primaryKeyFields.length > 0) {
            const primaryKeysStr = primaryKeyFields.join(',');
            parsedConfig['hoodie.table.recordkey.fields'] = primaryKeysStr;
            parsedConfig['hoodie.bucket.index.hash.field'] = primaryKeysStr;
            console.log('设置主键字段:', primaryKeysStr);
          }
          
          // 更新配置数据
          const updatedData = JSON.stringify(parsedConfig, null, 2);
          
          // 设置默认配置到状态
          setHoodieConfigValue(updatedData);
          form.setFieldsValue({ hoodieConfig: updatedData });
          
          // 解析为可视化编辑器格式
          const fields = parseHoodieConfig(updatedData);
          setHoodieConfigFields(fields);
          
          console.log('获取默认Hoodie配置成功', updatedData);
          message.success('获取默认配置成功，已自动设置主键字段');
        } catch (error) {
          console.error('解析默认Hoodie配置失败:', error);
          message.error('解析默认Hoodie配置失败');
        }
      },
      onError: (error) => {
        console.error('获取默认Hoodie配置失败:', error);
        message.error(`获取默认Hoodie配置失败: ${error.message}`);
      },
    }
  );

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

  // 生成格式化的Hoodie配置
  const getFormattedHoodieConfig = () => {
    try {
      if (hoodieConfigEditMode === 'visual') {
        return fieldsToHoodieConfig(hoodieConfigFields);
      } else {
        if (hoodieConfigValue) {
          return JSON.stringify(JSON.parse(hoodieConfigValue), null, 2);
        }
        return '';
      }
    } catch (error) {
      return hoodieConfigValue;
    }
  };

  // 处理文本编辑模式下的schema变化
  const handleSchemaTextChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    const value = e.target.value;
    setSchemaValue(value);
    form.setFieldsValue({ schema: value });
    
    // 尝试解析并更新可视化字段
    try {
      const parsedSchema = JSON.parse(value);
      if (parsedSchema.type === 'struct' && parsedSchema.fields) {
        // 将字段名转换为小写
                 const fieldsWithLowerCase = parsedSchema.fields.map((field: any) => ({
           ...field,
           name: field.name ? field.name.toLowerCase() : ''
         }));
        parsedSchema.fields = fieldsWithLowerCase;
        
        // 更新schema值
        const updatedSchemaValue = JSON.stringify(parsedSchema, null, 2);
        setSchemaValue(updatedSchemaValue);
        form.setFieldsValue({ schema: updatedSchemaValue });
        
        // 更新可视化字段
        const fields = parseTableSchema(updatedSchemaValue);
        setSchemaFields(fields);
      }
    } catch (error) {
      // 解析失败时不更新可视化字段
    }
  };

  // 处理文本编辑模式下的hoodie配置变化
  const handleHoodieConfigTextChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    const value = e.target.value;
    setHoodieConfigValue(value);
    form.setFieldsValue({ hoodieConfig: value });
    
    // 尝试解析并更新可视化字段
    try {
      const fields = parseHoodieConfig(value);
      setHoodieConfigFields(fields);
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

  // 处理Hoodie配置编辑模式切换
  const handleHoodieConfigEditModeChange = (mode: 'visual' | 'text') => {
    setHoodieConfigEditMode(mode);
    if (mode === 'visual') {
      // 切换到可视化模式时，尝试解析现有的配置
      try {
        if (hoodieConfigValue) {
          const fields = parseHoodieConfig(hoodieConfigValue);
          setHoodieConfigFields(fields);
        }
      } catch (error) {
        // 解析失败时保持现有字段
      }
    }
  };

  // 当modal打开时，初始化表单数据
  useEffect(() => {
    if (visible && record) {
      const values = {
        status: record.status,
        dbType: 'mysql', // 默认数据库类型，因为MetaTableDTO中没有此字段
        sourceDb: record.sourceDb,
        sourceTable: record.sourceTable,
        isPartitioned: record.isPartitioned,
        partitionExpr: record.partitionExpr,
        tags: record.tags,
        description: record.description,
        schema: record.schema,
        hoodieConfig: record.hoodieConfig || '{}', // 添加hoodie配置
      };
      
      form.setFieldsValue(values);
      setInitialValues(values); // 保存初始值
      setIsPartitioned(record.isPartitioned || false);
      setSchemaValue(record.schema || '');
      setHoodieConfigValue(record.hoodieConfig || '{}');
      
      // 解析表结构并将字段名转换为小写
      let schemaToUse = record.schema || '';
      if (schemaToUse) {
        try {
          const parsedSchema = JSON.parse(schemaToUse);
          if (parsedSchema.type === 'struct' && parsedSchema.fields) {
            parsedSchema.fields = parsedSchema.fields.map((field: any) => ({
              ...field,
              name: field.name ? field.name.toLowerCase() : ''
            }));
            schemaToUse = JSON.stringify(parsedSchema);
            setSchemaValue(schemaToUse);
            form.setFieldsValue({ schema: schemaToUse });
          }
        } catch (error) {
          console.error('解析schema失败:', error);
        }
      }
      
      const fields = parseTableSchema(schemaToUse);
      setSchemaFields(fields);
      
      // 解析Hoodie配置
      const hoodieFields = parseHoodieConfig(record.hoodieConfig || '{}');
      setHoodieConfigFields(hoodieFields);
    }
  }, [visible, record, form]);

  // 处理表单提交
  const handleSubmit = async () => {
    if (!record) return;
    
    try {
      const values = await form.validateFields();
      
      // 将sourceDb、sourceTable、dbType字段转换为小写
      if (values.sourceDb) {
        values.sourceDb = values.sourceDb.toLowerCase();
      }
      if (values.sourceTable) {
        values.sourceTable = values.sourceTable.toLowerCase();
      }
      if (values.dbType) {
        values.dbType = values.dbType.toLowerCase();
      }
      
      // 将schema字段中的字段名转换为小写
      if (values.schema) {
        try {
          const parsedSchema = JSON.parse(values.schema);
          if (parsedSchema.type === 'struct' && parsedSchema.fields) {
            parsedSchema.fields = parsedSchema.fields.map((field: any) => ({
              ...field,
              name: field.name ? field.name.toLowerCase() : ''
            }));
            values.schema = JSON.stringify(parsedSchema);
          }
        } catch (error) {
          console.error('转换schema字段名失败:', error);
        }
      }
      
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
                          schemaFields.length !== parseTableSchema(record?.schema || '').length ||
                          hoodieConfigFields.length !== parseHoodieConfig(record?.hoodieConfig || '{}').length;
    
    if (isFormTouched) {
      confirm({
        title: '确认关闭',
        content: '您已经修改了表单内容，确定要关闭吗？修改的内容将丢失。',
        okText: '确认',
        cancelText: '取消',
        onOk: () => {
          form.resetFields();
          setActiveTab('basic');
          setIsPartitioned(false);
          setSchemaEditMode('visual');
          setSchemaValue('');
          setSchemaFields([]);
          setEditingKey('');
          setInitialValues({});
          setHoodieConfigEditMode('visual');
          setHoodieConfigValue('');
          setHoodieConfigFields([]);
          setEditingHoodieConfigKey('');

          onCancel();
        },
      });
    } else {
      form.resetFields();
      setActiveTab('basic');
      setIsPartitioned(false);
      setSchemaEditMode('visual');
      setSchemaValue('');
      setSchemaFields([]);
      setEditingKey('');
      setInitialValues({});
      setHoodieConfigEditMode('visual');
      setHoodieConfigValue('');
      setHoodieConfigFields([]);
      setEditingHoodieConfigKey('');

      onCancel();
    }
  };

  // 处理模态框的 X 按钮和遮罩点击
  const handleModalCancel = () => {
    handleCancel();
  };

  // 渲染基本信息Tab
  const renderBasicInfoTab = () => (
    <>
      <Divider orientation="left">
        <Space>
          <InfoCircleOutlined />
          基本信息
        </Space>
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

      <Divider orientation="left">
        <Space>
          <DatabaseOutlined />
          数据源配置
        </Space>
      </Divider>

      <Row gutter={16}>
        <Col span={8}>
          <Form.Item
            label="数据库类型"
            name="dbType"
            rules={[{ required: true, message: '请选择数据库类型' }]}
          >
            <Select placeholder="请选择数据库类型">
              <Option value="tdsql">TDSQL</Option>
              <Option value="oracle">Oracle</Option>
              <Option value="mysql">MySQL</Option>
              <Option value="埋点">埋点</Option>
            </Select>
          </Form.Item>
        </Col>
        <Col span={8}>
          <Form.Item
            label="源数据库"
            name="sourceDb"
            rules={[{ required: true, message: '请输入源数据库' }]}
          >
            <Input 
              placeholder="请输入源数据库名称" 
              onChange={(e) => {
                const value = e.target.value.toLowerCase(); // 自动转换为小写
                form.setFieldsValue({ sourceDb: value });
              }}
            />
          </Form.Item>
        </Col>
        <Col span={8}>
          <Form.Item
            label="源表名"
            name="sourceTable"
            rules={[{ required: true, message: '请输入源表名' }]}
          >
            <Input 
              placeholder="请输入源表名称" 
              onChange={(e) => {
                const value = e.target.value.toLowerCase(); // 自动转换为小写
                form.setFieldsValue({ sourceTable: value });
              }}
            />
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
        {isPartitioned && (
          <Col span={12}>
            <Form.Item
              label="分区表达式"
              name="partitionExpr"
              rules={[{ required: isPartitioned, message: '请输入分区表达式' }]}
            >
              <AutoComplete
                options={partitionOptions}
                placeholder="请选择或输入分区表达式"
                style={{ width: '100%' }}
                onSelect={(value) => {
                  form.setFieldsValue({ partitionExpr: value });
                }}
              />
            </Form.Item>
          </Col>
        )}
      </Row>
    </>
  );

  // 渲染表结构Tab
  const renderSchemaTab = () => (
    <>
      <Form.Item
        label={
          <Space>
            <TableOutlined />
            表结构
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
                    scroll={{ y: 300 }}
                    locale={{
                      emptyText: '暂无字段信息，请点击"添加字段"添加表结构',
                    }}
                  />
                </div>
              ) : (
                <TextArea
                  rows={10}
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
    </>
  );

  // 渲染Hoodie配置Tab
  const renderHoodieConfigTab = () => (
    <>
      <Form.Item
        label={
          <Space>
            <SettingOutlined />
            Hoodie配置
          </Space>
        }
      >
        <Card size="small">
          <div style={{ marginBottom: 16 }}>
            <Radio.Group
              value={hoodieConfigEditMode}
              onChange={(e) => handleHoodieConfigEditModeChange(e.target.value)}
            >
              <Radio.Button value="visual">可视化编辑</Radio.Button>
              <Radio.Button value="text">文本编辑</Radio.Button>
            </Radio.Group>
          </div>
          
          <Tabs defaultActiveKey="editor" type="card">
            <TabPane tab="编辑器" key="editor">
              {/* 隐藏的hoodieConfig字段，用于存储JSON数据 */}
              <Form.Item
                name="hoodieConfig"
                hidden
              >
                <Input />
              </Form.Item>

              {hoodieConfigEditMode === 'visual' ? (
                <div>
                  <div style={{ marginBottom: 16 }}>
                    <Row gutter={8}>
                      <Col span={12}>
                        <Button
                          type="dashed"
                          onClick={handleAddHoodieConfig}
                          icon={<PlusOutlined />}
                          style={{ width: '100%' }}
                        >
                          添加配置
                        </Button>
                      </Col>
                      <Col span={12}>
                        <Button
                          type="primary"
                          onClick={getDefaultConfig}
                          loading={loadingDefaultConfig}
                          style={{ width: '100%' }}
                        >
                          获取默认配置
                        </Button>
                      </Col>
                    </Row>
                  </div>
                  <Table
                    columns={hoodieConfigColumns}
                    dataSource={hoodieConfigFields}
                    pagination={false}
                    size="small"
                    bordered
                    scroll={{ y: 300 }}
                    locale={{
                      emptyText: '暂无配置信息，请点击"添加配置"添加Hoodie配置',
                    }}
                  />
                </div>
              ) : (
                <TextArea
                  rows={10}
                  placeholder="请输入Hoodie配置JSON，格式如：{&quot;key&quot;: &quot;value&quot;}"
                  style={{ fontFamily: 'monospace' }}
                  value={hoodieConfigValue}
                  onChange={handleHoodieConfigTextChange}
                />
              )}
            </TabPane>
            
            <TabPane tab="JSON配置预览" key="preview">
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
                  {getFormattedHoodieConfig() || '// 请在编辑器中输入Hoodie配置或添加配置项'}
                </pre>
              </div>
            </TabPane>
          </Tabs>
        </Card>
      </Form.Item>
    </>
  );

  // 渲染其他信息Tab
  const renderOtherInfoTab = () => (
    <>
      <Divider orientation="left">
        <Space>
          <FileTextOutlined />
          其他信息
        </Space>
      </Divider>

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
          rows={4}
          placeholder="请输入表描述信息"
          maxLength={500}
          showCount
        />
      </Form.Item>
    </>
  );

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
      width={1200}
      destroyOnClose
    >
      <Form
        form={form}
        layout="vertical"
      >
        <Tabs 
          activeKey={activeTab} 
          onChange={setActiveTab}
          type="card"
          size="large"
        >
          <TabPane 
            tab={
              <Space>
                <InfoCircleOutlined />
                基本信息
              </Space>
            } 
            key="basic"
          >
            {renderBasicInfoTab()}
          </TabPane>
          
          <TabPane 
            tab={
              <Space>
                <TableOutlined />
                表结构
              </Space>
            } 
            key="schema"
          >
            {renderSchemaTab()}
          </TabPane>
          
          <TabPane 
            tab={
              <Space>
                <SettingOutlined />
                Hoodie配置
              </Space>
            } 
            key="hoodieConfig"
          >
            {renderHoodieConfigTab()}
          </TabPane>
          
          <TabPane 
            tab={
              <Space>
                <FileTextOutlined />
                其他信息
              </Space>
            } 
            key="other"
          >
            {renderOtherInfoTab()}
          </TabPane>
        </Tabs>
      </Form>


    </Modal>
  );
};

export default EditTableModal; 