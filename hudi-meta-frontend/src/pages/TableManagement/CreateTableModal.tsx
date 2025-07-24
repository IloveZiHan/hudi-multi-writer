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
  Tabs,
  Radio,
  Card,
  Table,
  Tag,
  Badge,
  Typography,
  Popconfirm,
  AutoComplete,
  Alert,
} from 'antd';
import { PlusOutlined, DatabaseOutlined, EditOutlined, DeleteOutlined, SettingOutlined, InfoCircleOutlined, TableOutlined, FileTextOutlined, EditFilled, CheckCircleOutlined } from '@ant-design/icons';
import { useRequest } from 'ahooks';
import tableApiService from '@services/tableApi';
import hoodieConfigApiService from '@services/hoodieConfigApi';
import { CreateTableRequest, TableStatus, SupportedDbTypes } from '../../types/api';
import { generateTableId, getAvailableDatabases } from '../../config/databaseMapping';

const { Option } = Select;
const { TextArea } = Input;
const { TabPane } = Tabs;
const { Text } = Typography;
const { confirm } = Modal;

interface CreateTableModalProps {
  visible: boolean;
  onCancel: () => void;
  onSuccess: () => void;
  initialData?: {
    id?: string;
    sourceDb?: string;
    sourceTable?: string;
    dbType?: string;
    schema?: string;
    description?: string;
    tags?: string;
    status?: TableStatus;
    partitionExpr?: string;
    isPartitioned?: boolean;
    hoodieConfig?: string;
  };
}

/**
 * 创建表模态框组件
 */
const CreateTableModal: React.FC<CreateTableModalProps> = ({
  visible,
  onCancel,
  onSuccess,
  initialData,
}) => {
  const [form] = Form.useForm();
  const [activeTab, setActiveTab] = useState('basic'); // 当前活动Tab
  // 分区表固定为true，不允许用户更改
  const [schemaEditMode, setSchemaEditMode] = useState<'visual' | 'text'>('text'); // 表结构编辑模式
  const [schemaValue, setSchemaValue] = useState(''); // 表结构值
  const [schemaFields, setSchemaFields] = useState<any[]>([]); // 可视化编辑的字段数据
  const [editingKey, setEditingKey] = useState<string>(''); // 正在编辑的字段key
  const [batchInputVisible, setBatchInputVisible] = useState(false); // 批量录入Modal是否可见
  const [batchInputValue, setBatchInputValue] = useState(''); // 批量录入的值
  
  // 选项卡访问状态
  const [visitedTabs, setVisitedTabs] = useState<Set<string>>(new Set(['basic'])); // 记录用户访问过的选项卡，默认包含basic
  
  // Hoodie Config 相关状态
  const [hoodieConfigEditMode, setHoodieConfigEditMode] = useState<'visual' | 'text'>('visual'); // Hoodie配置编辑模式
  const [hoodieConfigValue, setHoodieConfigValue] = useState(''); // Hoodie配置值
  const [hoodieConfigFields, setHoodieConfigFields] = useState<any[]>([]); // 可视化编辑的配置数据
  const [editingHoodieConfigKey, setEditingHoodieConfigKey] = useState<string>(''); // 正在编辑的配置key

  // 处理选项卡切换
  const handleTabChange = (key: string) => {
    setActiveTab(key);
    // 记录用户访问过的选项卡
    setVisitedTabs(prev => new Set(prev).add(key));
  };

  // 检查是否访问过必要的选项卡
  const checkTabsVisited = () => {
    const requiredTabs = ['schema', 'hoodieConfig', 'other'];
    const unvisitedTabs = requiredTabs.filter(tab => !visitedTabs.has(tab));
    return unvisitedTabs;
  };

  // 渲染选项卡标题（带确认状态）
  const renderTabTitle = (icon: React.ReactNode, title: string, tabKey: string) => {
    const isVisited = visitedTabs.has(tabKey);
    return (
      <Space>
        {icon}
        {title}
        {isVisited && tabKey !== 'basic' && (
          <CheckCircleOutlined style={{ color: '#52c41a' }} />
        )}
      </Space>
    );
  };

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

  // 当modal可见时，重置表单并获取默认配置
  useEffect(() => {
    if (visible) {
      form.resetFields();
      setActiveTab('basic');
      setSchemaEditMode('text');
      setSchemaValue('');
      setSchemaFields([]);
      setEditingKey('');
      setBatchInputVisible(false);
      setBatchInputValue('');
      setHoodieConfigEditMode('visual');
      setHoodieConfigValue('');
      setHoodieConfigFields([]);
      setEditingHoodieConfigKey('');
      setVisitedTabs(new Set(['basic'])); // 重置访问状态
      
      // 获取默认Hoodie配置
      getDefaultConfig();

      // 预填充初始数据
      if (initialData) {
        form.setFieldsValue({
          id: initialData.id,
          status: initialData.status || TableStatus.OFFLINE,
          dbType: initialData.dbType || 'tdsql',
          sourceDb: initialData.sourceDb,
          sourceTable: initialData.sourceTable,
          partitionExpr: initialData.partitionExpr || "trunc(create_time, 'year')",
          isPartitioned: initialData.isPartitioned !== undefined ? initialData.isPartitioned : true,
          description: initialData.description,
          tags: initialData.tags,
        });

        // 预填充schema
        if (initialData.schema) {
          try {
            const parsedSchema = JSON.parse(initialData.schema);
            if (parsedSchema.type === 'struct' && parsedSchema.fields) {
              const fields = parseTableSchema(initialData.schema);
              setSchemaFields(fields);
              setSchemaValue(JSON.stringify(parsedSchema, null, 2));
            } else {
              setSchemaValue(initialData.schema);
            }
          } catch (error) {
            console.error('解析初始schema失败:', error);
            setSchemaValue(initialData.schema);
          }
        }

        // 预填充hoodieConfig
        if (initialData.hoodieConfig) {
          try {
            const parsedConfig = JSON.parse(initialData.hoodieConfig);
            setHoodieConfigValue(JSON.stringify(parsedConfig, null, 2));
            const fields = parseHoodieConfig(initialData.hoodieConfig);
            setHoodieConfigFields(fields);
          } catch (error) {
            console.error('解析初始hoodieConfig失败:', error);
            setHoodieConfigValue(initialData.hoodieConfig);
          }
        }
      }
    }
  }, [visible, form]);

  // 当有schema或hoodie配置时，初始化可视化数据
  useEffect(() => {
    if (visible) {
      // 如果有schema值，解析为可视化字段
      if (schemaValue) {
        const fields = parseTableSchema(schemaValue);
        setSchemaFields(fields);
      }
      
      // 如果有hoodie配置值，解析为可视化字段
      if (hoodieConfigValue) {
        const hoodieFields = parseHoodieConfig(hoodieConfigValue);
        setHoodieConfigFields(hoodieFields);
      }
    }
  }, [visible, schemaValue, hoodieConfigValue]);

  // 当字段数据已设置时，确保表单字段始终同步（兜底机制）
  useEffect(() => {
    if (visible) {
      // 检查schema字段
      if (schemaFields.length > 0) {
        const schemaJson = fieldsToSchema(schemaFields);
        const currentSchema = form.getFieldValue('schema');
        if (!currentSchema || currentSchema !== schemaJson) {
          console.log('兜底同步schema字段:', schemaJson);
          form.setFieldsValue({ schema: schemaJson });
        }
      }
      
      // 检查hoodieConfig字段
      if (hoodieConfigFields.length > 0) {
        const configJson = fieldsToHoodieConfig(hoodieConfigFields);
        const currentConfig = form.getFieldValue('hoodieConfig');
        if (!currentConfig || currentConfig !== configJson) {
          console.log('兜底同步hoodieConfig字段:', configJson);
          form.setFieldsValue({ hoodieConfig: configJson });
        }
      }
    }
  }, [visible, schemaFields, hoodieConfigFields, form]);

  // 解析表结构 JSON
  const parseTableSchema = (schema: string) => {
    try {
      const parsed = JSON.parse(schema);
      if (parsed.type === 'struct' && parsed.fields) {
        return parsed.fields.map((field: any, index: number) => ({
          key: index.toString(),
          name: field.name ? field.name.toLowerCase() : '', // 转换为小写，处理空值
          type: field.type,
          nullable: field.nullable !== undefined ? field.nullable : true, // 确保nullable字段存在，默认为true
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

  // 创建复杂类型的辅助函数
  const createArrayType = (elementType: string, containsNull: boolean = true) => {
    return {
      type: 'array',
      elementType: elementType,
      containsNull: containsNull,
    };
  };

  const createMapType = (keyType: string, valueType: string, valueContainsNull: boolean = true) => {
    return {
      type: 'map',
      keyType: keyType,
      valueType: valueType,
      valueContainsNull: valueContainsNull,
    };
  };

  const createStructType = (fields: any[]) => {
    return {
      type: 'struct',
      fields: fields,
    };
  };

  // 验证和规范化Spark数据类型
  const normalizeSparkDataType = (dataType: string): string => {
    // 处理常见的类型别名和格式
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

    // 如果是简单类型映射
    if (typeMap[dataType.toLowerCase()]) {
      return typeMap[dataType.toLowerCase()];
    }

    // 处理decimal类型的格式：decimal(precision,scale)
    const decimalMatch = dataType.match(/^decimal\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)$/i);
    if (decimalMatch) {
      return `decimal(${decimalMatch[1]},${decimalMatch[2]})`;
    }

    // 处理char和varchar类型
    const charMatch = dataType.match(/^char\s*\(\s*(\d+)\s*\)$/i);
    if (charMatch) {
      return `char(${charMatch[1]})`;
    }

    const varcharMatch = dataType.match(/^varchar\s*\(\s*(\d+)\s*\)$/i);
    if (varcharMatch) {
      return `varchar(${varcharMatch[1]})`;
    }

    // 其他类型直接返回
    return dataType;
  };

  // 将字段数据转换为Schema JSON - 确保与Spark DataType.fromJson兼容
  const fieldsToSchema = (fields: any[]) => {
    const sparkSchemaFields = fields.map((field) => {
      // 处理复杂类型的解析
      const processFieldType = (fieldType: string): any => {
        if (typeof fieldType === 'string') {
          // 检查是否是复杂类型的JSON表示
          if (fieldType.startsWith('{') && fieldType.endsWith('}')) {
            try {
              return JSON.parse(fieldType);
            } catch (e) {
              // 如果解析失败，返回原始字符串
              return normalizeSparkDataType(fieldType);
            }
          }
          // 简单类型进行规范化
          return normalizeSparkDataType(fieldType);
        }
        // 如果已经是对象，直接返回
        return fieldType;
      };

      // 按照Spark DataType.parseStructField期望的字段顺序构建
      // Spark会通过JSortedObject按字母顺序排序，所以最终顺序是：metadata, name, nullable, type
      const fieldData: any = {
        metadata: {},
        name: field.name ? field.name.toLowerCase() : '', // 转换为小写，处理空值
        nullable: field.nullable !== undefined ? field.nullable : true, // 确保nullable字段存在，默认为true
        type: processFieldType(field.type),
      };
      
      // 添加comment到metadata中
      if (field.comment && field.comment.trim() !== '') {
        fieldData.metadata.comment = field.comment.trim();
      }
      
      return fieldData;
    });
    
    // 生成符合Spark期望格式的schema JSON
    const schema = {
      type: 'struct',
      fields: sparkSchemaFields,
    };
    
    return JSON.stringify(schema, null, 2);
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

  // 自动生成表ID
  const autoGenerateTableId = () => {
    const sourceDb = form.getFieldValue('sourceDb');
    const sourceTable = form.getFieldValue('sourceTable');
    
    if (sourceDb && sourceTable) {
      const generatedTableId = generateTableId(sourceDb, sourceTable);
      form.setFieldsValue({ id: generatedTableId });
      // 自动更新hoodie.table.name
      handleTableIdChange(generatedTableId);
      console.log('自动生成表ID:', generatedTableId);
    }
  };

  // 处理表ID变化时自动更新hoodie.table.name
  const handleTableIdChange = (tableId: string) => {
    if (!tableId) return;
    
    const lowercaseTableId = tableId.toLowerCase();
    
    // 更新hoodie配置中的hoodie.table.name
    if (hoodieConfigEditMode === 'visual' && hoodieConfigFields.length > 0) {
      const updatedFields = hoodieConfigFields.map(field => {
        if (field.configKey === 'hoodie.table.name') {
          return { ...field, configValue: lowercaseTableId };
        }
        return field;
      });
      
      // 如果没有找到hoodie.table.name配置，则添加一个
      const hasTableName = updatedFields.some(field => field.configKey === 'hoodie.table.name');
      if (!hasTableName) {
        updatedFields.push({
          key: Date.now().toString(),
          configKey: 'hoodie.table.name',
          configValue: lowercaseTableId,
        });
      }
      
      updateHoodieConfigFields(updatedFields);
    } else if (hoodieConfigEditMode === 'text' && hoodieConfigValue) {
      try {
        const parsedConfig = JSON.parse(hoodieConfigValue);
        parsedConfig['hoodie.table.name'] = lowercaseTableId;
        const updatedConfigValue = JSON.stringify(parsedConfig, null, 2);
        setHoodieConfigValue(updatedConfigValue);
        form.setFieldsValue({ hoodieConfig: updatedConfigValue });
      } catch (error) {
        console.error('更新hoodie.table.name失败:', error);
      }
    }
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

  // 批量录入字段
  const handleBatchInput = () => {
    setBatchInputVisible(true);
    setBatchInputValue('');
  };

  // 处理批量录入提交
  const handleBatchInputSubmit = () => {
    if (!batchInputValue.trim()) {
      message.warning('请输入字段信息');
      return;
    }

    try {
      // 解析批量输入的字段
      const fieldItems = batchInputValue.split(',').map(item => item.trim()).filter(item => item);
      const newFields = fieldItems.map((item, index) => {
        let fieldName = item;
        let fieldType = 'string';
        let fieldComment = '';

        // 支持格式: "字段名:类型:注释" 或 "字段名:类型" 或 "字段名"
        if (item.includes(':')) {
          const parts = item.split(':');
          fieldName = parts[0].trim().toLowerCase(); // 转换为小写
          if (parts.length > 1 && parts[1].trim()) {
            fieldType = parts[1].trim();
          }
          if (parts.length > 2 && parts[2].trim()) {
            fieldComment = parts[2].trim();
          }
        } else {
          fieldName = fieldName.toLowerCase(); // 转换为小写
        }

        return {
          key: (Date.now() + index).toString(),
          name: fieldName,
          type: fieldType,
          nullable: true, // 批量录入时默认为可为空
          comment: fieldComment,
        };
      });

      // 添加到现有字段列表
      const updatedFields = [...schemaFields, ...newFields];
      updateSchemaFields(updatedFields);

      // 关闭Modal并清空输入
      setBatchInputVisible(false);
      setBatchInputValue('');
      
      message.success(`成功添加 ${newFields.length} 个字段`);
    } catch (error) {
      message.error('批量录入失败，请检查输入格式');
    }
  };

  // 取消批量录入
  const handleBatchInputCancel = () => {
    setBatchInputVisible(false);
    setBatchInputValue('');
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
            parsedConfig['hoodie.datasource.write.recordkey.field'] = primaryKeysStr;
            parsedConfig['hoodie.bucket.index.hash.field'] = primaryKeysStr;
            console.log('设置主键字段:', primaryKeysStr);
          }
          
          // 自动设置hoodie.table.name为当前表ID
          const currentTableId = form.getFieldValue('id');
          if (currentTableId) {
            parsedConfig['hoodie.table.name'] = currentTableId.toLowerCase();
            console.log('自动设置hoodie.table.name:', currentTableId.toLowerCase());
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
          message.success('获取默认配置成功，已自动设置主键字段和表名');
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

  // 创建表请求
  const { loading, run: createTable } = useRequest(
    (data: CreateTableRequest) => tableApiService.createTable(data),
    {
      manual: true,
      onSuccess: () => {
        message.success('创建表成功');
        form.resetFields();
        onSuccess();
      },
      onError: (error) => {
        message.error(`创建表失败: ${error.message}`);
      },
    }
  );

  // 处理表单提交
  const handleSubmit = async () => {
    try {
      // 检查用户是否访问过必要的选项卡
      const unvisitedTabs = checkTabsVisited();
      if (unvisitedTabs.length > 0) {
        const tabNames = {
          schema: '表结构',
          hoodieConfig: 'Hoodie配置',
          other: '其他信息'
        };
        const unvisitedTabNames = unvisitedTabs.map(tab => tabNames[tab as keyof typeof tabNames]).join('、');
        message.warning(`请先查看并确认以下选项卡的内容：${unvisitedTabNames}`);
        
        // 自动切换到第一个未访问的选项卡
        setActiveTab(unvisitedTabs[0]);
        return;
      }
      
      console.log('开始处理表单提交');
      console.log('当前schemaEditMode:', schemaEditMode);
      console.log('当前schemaFields:', schemaFields);
      console.log('当前schemaValue:', schemaValue);
      
      // 在表单验证之前，确保schema字段是最新的
      if (schemaEditMode === 'visual' && schemaFields.length > 0) {
        const schemaJson = fieldsToSchema(schemaFields);
        console.log('从可视化字段生成的schema:', schemaJson);
        form.setFieldsValue({ schema: schemaJson });
      } else if (schemaEditMode === 'text' && schemaValue) {
        // 如果是文本编辑模式，确保使用当前的schemaValue
        console.log('使用文本编辑模式的schema:', schemaValue);
        form.setFieldsValue({ schema: schemaValue });
      } else if (schemaValue) {
        // 兜底：如果有schemaValue，使用它
        console.log('使用兜底的schema:', schemaValue);
        form.setFieldsValue({ schema: schemaValue });
      }
      
      // 在表单验证之前，确保hoodieConfig字段是最新的
      if (hoodieConfigEditMode === 'visual' && hoodieConfigFields.length > 0) {
        const configJson = fieldsToHoodieConfig(hoodieConfigFields);
        console.log('从可视化字段生成的hoodieConfig:', configJson);
        form.setFieldsValue({ hoodieConfig: configJson });
      } else if (hoodieConfigEditMode === 'text' && hoodieConfigValue) {
        // 如果是文本编辑模式，确保使用当前的hoodieConfigValue
        console.log('使用文本编辑模式的hoodieConfig:', hoodieConfigValue);
        form.setFieldsValue({ hoodieConfig: hoodieConfigValue });
      } else if (hoodieConfigValue) {
        // 兜底：如果有hoodieConfigValue，使用它
        console.log('使用兜底的hoodieConfig:', hoodieConfigValue);
        form.setFieldsValue({ hoodieConfig: hoodieConfigValue });
      }
      
      // 强制确保schema字段有值，防止表单验证失败
      const currentFormValues = form.getFieldsValue();
      if (!currentFormValues.schema) {
        console.log('表单schema字段为空，强制设置值');
        if (schemaFields.length > 0) {
          const schemaJson = fieldsToSchema(schemaFields);
          console.log('使用可视化字段生成schema:', schemaJson);
          form.setFieldsValue({ schema: schemaJson });
        } else if (schemaValue) {
          console.log('使用schemaValue:', schemaValue);
          form.setFieldsValue({ schema: schemaValue });
        } else {
          console.log('没有可用的schema数据，设置空的schema结构');
          const emptySchema = JSON.stringify({ type: 'struct', fields: [] }, null, 2);
          form.setFieldsValue({ schema: emptySchema });
        }
      }
      
      // 强制确保hoodieConfig字段有值
      if (!currentFormValues.hoodieConfig) {
        console.log('表单hoodieConfig字段为空，强制设置值');
        if (hoodieConfigFields.length > 0) {
          const configJson = fieldsToHoodieConfig(hoodieConfigFields);
          console.log('使用可视化字段生成hoodieConfig:', configJson);
          form.setFieldsValue({ hoodieConfig: configJson });
        } else if (hoodieConfigValue) {
          console.log('使用hoodieConfigValue:', hoodieConfigValue);
          form.setFieldsValue({ hoodieConfig: hoodieConfigValue });
        } else {
          console.log('没有可用的hoodieConfig数据，设置空对象');
          form.setFieldsValue({ hoodieConfig: '{}' });
        }
      }
      
      console.log('强制设置后的表单值:', form.getFieldsValue());
      
      const values = await form.validateFields();
      console.log('表单验证后的values:', values);
      
      // 将id字段转换为小写
      if (values.id) {
        values.id = values.id.toLowerCase();
      }
      
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
      
      // 将schema字段中的字段名转换为小写，并确保包含nullable字段
      if (values.schema) {
        try {
          const parsedSchema = JSON.parse(values.schema);
          if (parsedSchema.type === 'struct' && parsedSchema.fields) {
            parsedSchema.fields = parsedSchema.fields.map((field: any) => ({
              ...field,
              name: field.name ? field.name.toLowerCase() : '',
              nullable: field.nullable !== undefined ? field.nullable : true, // 确保nullable字段存在，默认为true
            }));
            values.schema = JSON.stringify(parsedSchema);
          }
        } catch (error) {
          console.error('转换schema字段名失败:', error);
        }
      }
      
      // 自动设置hoodie配置中的hoodie.table.name为表ID
      if (values.hoodieConfig && values.id) {
        try {
          const parsedHoodieConfig = JSON.parse(values.hoodieConfig);
          parsedHoodieConfig['hoodie.table.name'] = values.id.toLowerCase();
          values.hoodieConfig = JSON.stringify(parsedHoodieConfig);
          console.log('自动设置hoodie.table.name为:', values.id.toLowerCase());
        } catch (error) {
          console.error('设置hoodie.table.name失败:', error);
        }
      }
      
      console.log('最终提交的values:', values);
      await createTable(values);
    } catch (error) {
      // 表单验证失败
      console.error('表单验证失败:', error);
    }
  };

  // 处理取消
  const handleCancel = () => {
    // 检查表单是否被修改过
    const isFormTouched = form.isFieldsTouched() || schemaFields.length > 0 || schemaValue.trim() !== '' || hoodieConfigFields.length > 0 || hoodieConfigValue.trim() !== '';
    
    if (isFormTouched) {
      confirm({
        title: '确认关闭',
        content: '您已经修改了表单内容，确定要关闭吗？修改的内容将丢失。',
        okText: '确认',
        cancelText: '取消',
        onOk: () => {
          form.resetFields();
          setActiveTab('basic');
          setSchemaEditMode('text');
          setSchemaValue('');
          setSchemaFields([]);
          setEditingKey('');
          setBatchInputVisible(false);
          setBatchInputValue('');
          setHoodieConfigEditMode('visual');
          setHoodieConfigValue('');
          setHoodieConfigFields([]);
          setEditingHoodieConfigKey('');
          setVisitedTabs(new Set(['basic'])); // 重置访问状态
          onCancel();
        },
      });
    } else {
      form.resetFields();
      setActiveTab('basic');
      setSchemaEditMode('text');
      setSchemaValue('');
      setSchemaFields([]);
      setEditingKey('');
      setBatchInputVisible(false);
      setBatchInputValue('');
      setHoodieConfigEditMode('visual');
      setHoodieConfigValue('');
      setHoodieConfigFields([]);
      setEditingHoodieConfigKey('');
      setVisitedTabs(new Set(['basic'])); // 重置访问状态
      onCancel();
    }
  };

  // 处理模态框的 X 按钮和遮罩点击
  const handleModalCancel = () => {
    handleCancel();
  };

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
        // 将字段名转换为小写，并确保每个字段都包含nullable字段
        const fieldsWithLowerCase = parsedSchema.fields.map((field: any) => ({
          ...field,
          name: field.name ? field.name.toLowerCase() : '',
          nullable: field.nullable !== undefined ? field.nullable : true, // 确保nullable字段存在，默认为true
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
          <Form.Item
            label="表ID"
            name="id"
            rules={[
              { required: true, message: '请输入表ID' },
              { pattern: /^[a-zA-Z][a-zA-Z0-9_]*$/, message: '表ID必须以字母开头，只能包含字母、数字和下划线' },
            ]}
          >
            <Input 
              placeholder="请输入表ID或自动生成" 
              onChange={(e) => {
                const value = e.target.value.toLowerCase(); // 自动转换为小写
                form.setFieldsValue({ id: value });
                // 自动更新hoodie.table.name
                handleTableIdChange(value);
              }}
            />
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
            <AutoComplete
              placeholder="请输入源数据库名称" 
              options={getAvailableDatabases().map(db => ({ label: db, value: db }))}
              onChange={(value) => {
                const normalizedValue = value.toLowerCase(); // 自动转换为小写
                form.setFieldsValue({ sourceDb: normalizedValue });
                // 自动生成表ID
                autoGenerateTableId();
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
                // 自动生成表ID
                autoGenerateTableId();
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
            <div style={{ 
              padding: '4px 11px', 
              border: '1px solid #d9d9d9', 
              borderRadius: '6px',
              backgroundColor: '#f6ffed',
              color: '#52c41a',
              display: 'inline-flex',
              alignItems: 'center',
              fontSize: '14px'
            }}>
              <CheckCircleOutlined style={{ marginRight: '8px' }} />
              是
            </div>
          </Form.Item>
        </Col>
        <Col span={12}>
          <Form.Item
            label="分区表达式"
            name="partitionExpr"
            rules={[{ required: true, message: '请输入分区表达式' }]}
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
                    <Row gutter={8}>
                      <Col span={12}>
                        <Button
                          type="dashed"
                          onClick={handleAddField}
                          icon={<PlusOutlined />}
                          style={{ width: '100%' }}
                        >
                          添加字段
                        </Button>
                      </Col>
                      <Col span={12}>
                        <Button
                          type="dashed"
                          onClick={handleBatchInput}
                          icon={<PlusOutlined />}
                          style={{ width: '100%' }}
                        >
                          批量录入
                        </Button>
                      </Col>
                    </Row>
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
          <PlusOutlined />
          创建表
        </Space>
      }
      open={visible}
      onCancel={handleModalCancel}
      footer={[
        <Button key="cancel" onClick={handleCancel}>
          取消
        </Button>,
        <Button key="submit" type="primary" loading={loading} onClick={handleSubmit}>
          创建
        </Button>,
      ]}
      width={1200}
      destroyOnClose
    >
      <Form
        form={form}
        layout="vertical"
        initialValues={{
          status: TableStatus.OFFLINE,
          isPartitioned: true, // 默认设置为true
          partitionExpr: "trunc(create_time, 'year')", // 默认使用按年分区
        }}
      >
        <Alert
          message="创建表前必须查看所有选项卡"
          description="请依次查看和确认「表结构」、「Hoodie配置」、「其他信息」等选项卡的内容，确保配置正确后再创建表。"
          type="info"
          showIcon
          style={{ marginBottom: 16 }}
        />
        <Tabs 
          activeKey={activeTab} 
          onChange={handleTabChange}
          type="card"
          size="large"
        >
          <TabPane 
            tab={renderTabTitle(<InfoCircleOutlined />, '基本信息', 'basic')}
            key="basic"
          >
            {renderBasicInfoTab()}
          </TabPane>
          
          <TabPane 
            tab={renderTabTitle(<TableOutlined />, '表结构', 'schema')}
            key="schema"
          >
            {renderSchemaTab()}
          </TabPane>
          
          <TabPane 
            tab={renderTabTitle(<SettingOutlined />, 'Hoodie配置', 'hoodieConfig')}
            key="hoodieConfig"
          >
            {renderHoodieConfigTab()}
          </TabPane>
          
          <TabPane 
            tab={renderTabTitle(<FileTextOutlined />, '其他信息', 'other')}
            key="other"
          >
            {renderOtherInfoTab()}
          </TabPane>
        </Tabs>
      </Form>

      {/* 批量录入Modal */}
      <Modal
        title="批量录入字段"
        open={batchInputVisible}
        onOk={handleBatchInputSubmit}
        onCancel={handleBatchInputCancel}
        okText="确定"
        cancelText="取消"
        width={600}
      >
        <div style={{ marginBottom: 16 }}>
          <p style={{ marginBottom: 8 }}>
            <strong>输入格式说明：</strong>
          </p>
          <ul style={{ paddingLeft: 20, marginBottom: 0 }}>
            <li>简单格式：<code>字段名1, 字段名2, 字段名3</code></li>
            <li>带类型格式：<code>字段名1:类型1, 字段名2:类型2</code></li>
            <li>完整格式：<code>字段名1:类型1:注释1, 字段名2:类型2:注释2</code></li>
          </ul>
          <p style={{ marginTop: 8, color: '#666', fontSize: '12px' }}>
            支持的数据类型：string, int, bigint, double, boolean, timestamp, date, decimal
          </p>
        </div>
        <TextArea
          rows={6}
          placeholder="示例：
name:string:用户姓名, 
age:int:年龄, 
email:string:邮箱地址,
created_at:timestamp:创建时间"
          value={batchInputValue}
          onChange={(e) => setBatchInputValue(e.target.value)}
          style={{ fontFamily: 'monospace' }}
        />
      </Modal>
    </Modal>
  );
};

export default CreateTableModal; 