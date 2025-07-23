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
  Alert,
} from 'antd';
import { EditOutlined, DatabaseOutlined, PlusOutlined, DeleteOutlined, SettingOutlined, InfoCircleOutlined, TableOutlined, FileTextOutlined, CheckCircleOutlined } from '@ant-design/icons';
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

  // 添加新字段
  const handleAddField = () => {
    const newField = {
      key: Date.now().toString(),
      name: '',
      type: 'string',
      nullable: true, // 新字段默认为可为空
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
            parsedConfig['hoodie.datasource.write.recordkey.field'] = primaryKeysStr;
            parsedConfig['hoodie.bucket.index.hash.field'] = primaryKeysStr;
            console.log('设置主键字段:', primaryKeysStr);
          }
          
          // 自动设置hoodie.table.name为当前表ID
          if (record?.id) {
            parsedConfig['hoodie.table.name'] = record.id.toLowerCase();
            console.log('自动设置hoodie.table.name:', record.id.toLowerCase());
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

  // 当modal打开时，初始化表单数据
  useEffect(() => {
    if (visible && record) {
      console.log('EditTableModal 初始化，接收到的record:', record);
      
      // 重置已访问的选项卡状态
      setActiveTab('basic');
      setVisitedTabs(new Set(['basic'])); // 确保basic被访问
      
      // 验证并规范化数据库类型
      const normalizeDbType = (sourceDb?: string) => {
        const dbName = sourceDb?.toLowerCase() || '';
        if (dbName.includes('mysql') || dbName.includes('tdsql')) return 'mysql';
        if (dbName.includes('oracle')) return 'oracle';
        if (dbName.includes('埋点')) return '埋点';
        return 'mysql'; // 默认类型
      };
      
      const values = {
        status: record.status,
        dbType: normalizeDbType(record.sourceDb), // 根据源数据库智能推断数据库类型
        sourceDb: record.sourceDb?.toLowerCase() || '',
        sourceTable: record.sourceTable?.toLowerCase() || '',
        isPartitioned: record.isPartitioned || false,
        partitionExpr: record.partitionExpr || "trunc(create_time, 'year')", // 默认使用按年分区
        tags: record.tags || '从业务系统导入',
        description: record.description || `从业务表导入的Hudi表 (${record.id})`,
        schema: record.schema || '',
        hoodieConfig: record.hoodieConfig || '{}', // 添加hoodie配置
      };
      
      console.log('设置表单初始值:', values);
      form.setFieldsValue(values);
      setInitialValues(values); // 保存初始值
      setIsPartitioned(values.isPartitioned);
      
      // 解析并规范化Schema
      let schemaToUse = record.schema || '';
      if (schemaToUse) {
        try {
          // 检查是否已经是有效的JSON格式
          let parsedSchema;
          if (schemaToUse.startsWith('{')) {
            parsedSchema = JSON.parse(schemaToUse);
          } else {
            // 如果不是JSON格式，尝试创建一个基础的schema结构
            console.warn('Schema不是有效的JSON格式，创建基础结构:', schemaToUse);
            parsedSchema = {
              type: 'struct',
              fields: [
                {
                  metadata: {},
                  name: 'id',
                  nullable: false,
                  type: 'string'
                }
              ]
            };
          }
          
          if (parsedSchema.type === 'struct' && parsedSchema.fields) {
            // 规范化字段信息
            parsedSchema.fields = parsedSchema.fields.map((field: any) => ({
              metadata: field.metadata || (field.comment ? { comment: field.comment } : {}),
              name: field.name ? field.name.toLowerCase() : '',
              nullable: field.nullable !== undefined ? field.nullable : true,
              type: field.type || 'string',
            }));
            
            schemaToUse = JSON.stringify(parsedSchema, null, 2);
            setSchemaValue(schemaToUse);
            form.setFieldsValue({ schema: schemaToUse });
            console.log('规范化后的schema:', schemaToUse);
          }
        } catch (error) {
          console.error('解析schema失败:', error);
          // 创建一个默认的schema结构
          const defaultSchema = {
            type: 'struct',
            fields: [
              {
                metadata: {},
                name: 'id',
                nullable: false,
                type: 'string'
              }
            ]
          };
          schemaToUse = JSON.stringify(defaultSchema, null, 2);
          setSchemaValue(schemaToUse);
          form.setFieldsValue({ schema: schemaToUse });
          message.warning('原始schema格式有误，已创建默认结构，请手动调整');
        }
      } else {
        // 如果没有schema，创建一个基本的默认结构
        const defaultSchema = {
          type: 'struct',
          fields: [
            {
              metadata: {},
              name: 'id',
              nullable: false,
              type: 'string'
            }
          ]
        };
        schemaToUse = JSON.stringify(defaultSchema, null, 2);
        setSchemaValue(schemaToUse);
        form.setFieldsValue({ schema: schemaToUse });
        console.log('创建默认schema结构');
      }
      
      const fields = parseTableSchema(schemaToUse);
      setSchemaFields(fields);
      
      // 初始化Hoodie配置，并自动设置关键参数
      let hoodieConfigToUse = record.hoodieConfig || '{}';
      try {
        const parsedHoodieConfig = JSON.parse(hoodieConfigToUse);
        
        // 自动设置hoodie.table.name为表ID（小写）
        parsedHoodieConfig['hoodie.table.name'] = record.id.toLowerCase();
        
        // 自动设置主键字段（基于schema中的字段）
        const primaryKeyFields = getPrimaryKeyFieldsFromSchema(fields);
        if (primaryKeyFields.length > 0) {
          const primaryKeysStr = primaryKeyFields.join(',');
          parsedHoodieConfig['hoodie.table.recordkey.fields'] = primaryKeysStr;
          parsedHoodieConfig['hoodie.datasource.write.recordkey.field'] = primaryKeysStr;
          parsedHoodieConfig['hoodie.bucket.index.hash.field'] = primaryKeysStr;
          console.log('自动设置主键字段:', primaryKeysStr);
        }
        
        // 设置一些常用的默认配置
        if (!parsedHoodieConfig['hoodie.table.type']) {
          parsedHoodieConfig['hoodie.table.type'] = 'COPY_ON_WRITE';
        }
        if (!parsedHoodieConfig['hoodie.datasource.write.operation']) {
          parsedHoodieConfig['hoodie.datasource.write.operation'] = 'upsert';
        }
        
        hoodieConfigToUse = JSON.stringify(parsedHoodieConfig, null, 2);
        setHoodieConfigValue(hoodieConfigToUse);
        form.setFieldsValue({ hoodieConfig: hoodieConfigToUse });
        console.log('自动配置的Hoodie参数:', parsedHoodieConfig);
      } catch (error) {
        console.error('解析并设置Hoodie配置失败:', error);
        // 设置基础的Hoodie配置
        const basicConfig = {
          'hoodie.table.name': record.id.toLowerCase(),
          'hoodie.table.type': 'COPY_ON_WRITE',
          'hoodie.datasource.write.operation': 'upsert'
        };
        hoodieConfigToUse = JSON.stringify(basicConfig, null, 2);
        setHoodieConfigValue(hoodieConfigToUse);
        form.setFieldsValue({ hoodieConfig: hoodieConfigToUse });
      }
      
      const hoodieFields = parseHoodieConfig(hoodieConfigToUse);
      setHoodieConfigFields(hoodieFields);
      
      console.log('EditTableModal 初始化完成');
    }
  }, [visible, record, form]);

  // 从schema字段中获取主键字段
  const getPrimaryKeyFieldsFromSchema = (fields: any[]): string[] => {
    const primaryKeyFields: string[] = [];
    
    fields.forEach(field => {
      if (field.name && (
        field.name.toLowerCase() === 'id' ||
        field.name.toLowerCase().includes('key') ||
        field.name.toLowerCase().includes('primary') ||
        !field.nullable // 非空字段可能是主键
      )) {
        primaryKeyFields.push(field.name.toLowerCase());
      }
    });
    
    // 如果没有找到明确的主键字段，使用第一个字段作为主键
    if (primaryKeyFields.length === 0 && fields.length > 0 && fields[0].name) {
      primaryKeyFields.push(fields[0].name.toLowerCase());
    }
    
    return [...new Set(primaryKeyFields)]; // 去重
  };

  // 处理表单提交
  const handleSubmit = async () => {
    if (!record) return;
    
         // 检查是否访问过必要的选项卡
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
      if (values.hoodieConfig && record.id) {
        try {
          const parsedHoodieConfig = JSON.parse(values.hoodieConfig);
          parsedHoodieConfig['hoodie.table.name'] = record.id.toLowerCase();
          values.hoodieConfig = JSON.stringify(parsedHoodieConfig);
          console.log('自动设置hoodie.table.name为:', record.id.toLowerCase());
        } catch (error) {
          console.error('设置hoodie.table.name失败:', error);
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
          setVisitedTabs(new Set(['basic'])); // 重置访问状态
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
      setVisitedTabs(new Set(['basic'])); // 重置访问状态
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
        <Alert
          message="编辑表前必须查看所有选项卡"
          description="请依次查看和确认「表结构」、「Hoodie配置」、「其他信息」等选项卡的内容，确保配置正确后再保存修改。"
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


    </Modal>
  );
};

export default EditTableModal; 