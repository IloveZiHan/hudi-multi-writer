import React, { useState } from 'react';
import {
  Modal,
  Form,
  Input,
  Button,
  message,
  Space,
  Tabs,
  InputNumber,
  Card,
  Row,
  Col,
  Switch,
  Tooltip,
  Select,
} from 'antd';
import { 
  PlusOutlined, 
  AppstoreOutlined, 
  SettingOutlined, 
  CodeOutlined,
  InfoCircleOutlined,
} from '@ant-design/icons';
import { useRequest } from 'ahooks';
import applicationApiService from '@services/applicationApi';
import { CreateApplicationRequest } from '@services/applicationApi';

const { TextArea } = Input;
const { Option } = Select;

interface CreateApplicationModalProps {
  visible: boolean;
  onCancel: () => void;
  onSuccess: () => void;
}

// Spark 默认配置
const DEFAULT_SPARK_CONFIG: Record<string, any> = {
  // Driver 配置
  'spark.driver.memory': '2g',
  'spark.driver.cores': 2,
  'spark.driver.maxResultSize': '2g',
  
  // Executor 配置
  'spark.executor.memory': '4g',
  'spark.executor.cores': 4,
  'spark.executor.instances': 8,
  'spark.executor.memoryFraction': 0.8,
  
  // 其他常用配置
  'spark.sql.adaptive.enabled': true,
  'spark.sql.adaptive.coalescePartitions.enabled': true,
  'spark.sql.adaptive.coalescePartitions.minPartitionNum': 1,
  'spark.sql.adaptive.coalescePartitions.initialPartitionNum': 200,
  'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
  'spark.sql.hive.convertMetastoreParquet': false,
  'spark.sql.parquet.filterPushdown': true,
  'spark.sql.parquet.mergeSchema': false,
};

/**
 * 创建应用程序模态框组件
 */
const CreateApplicationModal: React.FC<CreateApplicationModalProps> = ({
  visible,
  onCancel,
  onSuccess,
}) => {
  const [form] = Form.useForm();
  const [activeTab, setActiveTab] = useState('basic');
  const [configMode, setConfigMode] = useState<'visual' | 'json'>('visual');

  // 提交请求
  const { loading, run: createApplication } = useRequest(
    applicationApiService.createApplication,
    {
      manual: true,
      onSuccess: () => {
        message.success('应用程序创建成功！');
        form.resetFields();
        onSuccess();
      },
      onError: (error) => {
        message.error(`创建失败: ${error.message}`);
      },
    }
  );

  // 构建配置 JSON
  const buildConfigJson = (values: any) => {
    const sparkConfig: Record<string, any> = {};
    
    // Driver 配置
    if (values['spark.driver.memory']) sparkConfig['spark.driver.memory'] = values['spark.driver.memory'];
    if (values['spark.driver.cores']) sparkConfig['spark.driver.cores'] = values['spark.driver.cores'];
    if (values['spark.driver.maxResultSize']) sparkConfig['spark.driver.maxResultSize'] = values['spark.driver.maxResultSize'];
    
    // Executor 配置
    if (values['spark.executor.memory']) sparkConfig['spark.executor.memory'] = values['spark.executor.memory'];
    if (values['spark.executor.cores']) sparkConfig['spark.executor.cores'] = values['spark.executor.cores'];
    if (values['spark.executor.instances']) sparkConfig['spark.executor.instances'] = values['spark.executor.instances'];
    if (values['spark.executor.memoryFraction']) sparkConfig['spark.executor.memoryFraction'] = values['spark.executor.memoryFraction'];
    
    // 其他配置
    if (values['spark.sql.adaptive.enabled'] !== undefined) sparkConfig['spark.sql.adaptive.enabled'] = values['spark.sql.adaptive.enabled'];
    if (values['spark.sql.adaptive.coalescePartitions.enabled'] !== undefined) sparkConfig['spark.sql.adaptive.coalescePartitions.enabled'] = values['spark.sql.adaptive.coalescePartitions.enabled'];
    if (values['spark.sql.adaptive.coalescePartitions.minPartitionNum']) sparkConfig['spark.sql.adaptive.coalescePartitions.minPartitionNum'] = values['spark.sql.adaptive.coalescePartitions.minPartitionNum'];
    if (values['spark.sql.adaptive.coalescePartitions.initialPartitionNum']) sparkConfig['spark.sql.adaptive.coalescePartitions.initialPartitionNum'] = values['spark.sql.adaptive.coalescePartitions.initialPartitionNum'];
    if (values['spark.serializer']) sparkConfig['spark.serializer'] = values['spark.serializer'];
    if (values['spark.sql.hive.convertMetastoreParquet'] !== undefined) sparkConfig['spark.sql.hive.convertMetastoreParquet'] = values['spark.sql.hive.convertMetastoreParquet'];
    if (values['spark.sql.parquet.filterPushdown'] !== undefined) sparkConfig['spark.sql.parquet.filterPushdown'] = values['spark.sql.parquet.filterPushdown'];
    if (values['spark.sql.parquet.mergeSchema'] !== undefined) sparkConfig['spark.sql.parquet.mergeSchema'] = values['spark.sql.parquet.mergeSchema'];
    
    return JSON.stringify(sparkConfig, null, 2);
  };

  // 处理提交
  const handleSubmit = async () => {
    try {
      const values = await form.validateFields();
      
      let confStr = '';
      if (configMode === 'visual') {
        confStr = buildConfigJson(values);
      } else {
        confStr = values.conf?.trim() || '';
      }
      
      const request: CreateApplicationRequest = {
        name: values.name.trim(),
        description: values.description?.trim(),
        conf: confStr,
      };
      await createApplication(request);
    } catch (error) {
      console.error('表单验证失败:', error);
    }
  };

  // 处理取消
  const handleCancel = () => {
    form.resetFields();
    setActiveTab('basic');
    setConfigMode('visual');
    onCancel();
  };

  // 重置为默认配置
  const handleResetToDefault = () => {
    Object.keys(DEFAULT_SPARK_CONFIG).forEach(key => {
      form.setFieldValue(key, DEFAULT_SPARK_CONFIG[key]);
    });
    message.success('已重置为默认配置');
  };

  return (
    <Modal
      title={
        <Space>
          <PlusOutlined />
          创建应用程序
        </Space>
      }
      open={visible}
      onCancel={handleCancel}
      footer={[
        <Button key="cancel" onClick={handleCancel}>
          取消
        </Button>,
        <Button 
          key="submit" 
          type="primary" 
          loading={loading} 
          onClick={handleSubmit}
          icon={<AppstoreOutlined />}
        >
          创建
        </Button>,
      ]}
      width={800}
      destroyOnClose
    >
      <Form
        form={form}
        layout="vertical"
        initialValues={{
          name: '',
          description: '',
          conf: JSON.stringify(DEFAULT_SPARK_CONFIG, null, 2),
          
          // Spark Driver 默认配置
          'spark.driver.memory': DEFAULT_SPARK_CONFIG['spark.driver.memory'],
          'spark.driver.cores': DEFAULT_SPARK_CONFIG['spark.driver.cores'],
          'spark.driver.maxResultSize': DEFAULT_SPARK_CONFIG['spark.driver.maxResultSize'],
          
          // Spark Executor 默认配置
          'spark.executor.memory': DEFAULT_SPARK_CONFIG['spark.executor.memory'],
          'spark.executor.cores': DEFAULT_SPARK_CONFIG['spark.executor.cores'],
          'spark.executor.instances': DEFAULT_SPARK_CONFIG['spark.executor.instances'],
          'spark.executor.memoryFraction': DEFAULT_SPARK_CONFIG['spark.executor.memoryFraction'],
          
          // 其他默认配置
          'spark.sql.adaptive.enabled': DEFAULT_SPARK_CONFIG['spark.sql.adaptive.enabled'],
          'spark.sql.adaptive.coalescePartitions.enabled': DEFAULT_SPARK_CONFIG['spark.sql.adaptive.coalescePartitions.enabled'],
          'spark.sql.adaptive.coalescePartitions.minPartitionNum': DEFAULT_SPARK_CONFIG['spark.sql.adaptive.coalescePartitions.minPartitionNum'],
          'spark.sql.adaptive.coalescePartitions.initialPartitionNum': DEFAULT_SPARK_CONFIG['spark.sql.adaptive.coalescePartitions.initialPartitionNum'],
          'spark.serializer': DEFAULT_SPARK_CONFIG['spark.serializer'],
          'spark.sql.hive.convertMetastoreParquet': DEFAULT_SPARK_CONFIG['spark.sql.hive.convertMetastoreParquet'],
          'spark.sql.parquet.filterPushdown': DEFAULT_SPARK_CONFIG['spark.sql.parquet.filterPushdown'],
          'spark.sql.parquet.mergeSchema': DEFAULT_SPARK_CONFIG['spark.sql.parquet.mergeSchema'],
        }}
      >
        <Tabs
          activeKey={activeTab}
          onChange={setActiveTab}
          items={[
            {
              key: 'basic',
              label: (
                <span>
                  <AppstoreOutlined />
                  基本信息
                </span>
              ),
              children: (
                <>
                  <Form.Item
                    label="应用名称"
                    name="name"
                    rules={[
                      { required: true, message: '请输入应用名称' },
                      { min: 2, max: 100, message: '应用名称长度应在2-100个字符之间' },
                      { 
                        pattern: /^[a-zA-Z0-9_-]+$/, 
                        message: '应用名称只能包含字母、数字、下划线和连字符' 
                      },
                    ]}
                  >
                    <Input
                      placeholder="请输入应用名称，如：user-service"
                      showCount
                      maxLength={100}
                    />
                  </Form.Item>

                  <Form.Item
                    label="描述信息"
                    name="description"
                    rules={[
                      { max: 1000, message: '描述信息长度不能超过1000个字符' },
                    ]}
                  >
                    <TextArea
                      placeholder="请输入应用程序的描述信息"
                      rows={4}
                      showCount
                      maxLength={1000}
                    />
                  </Form.Item>
                </>
              ),
            },
            {
              key: 'config',
              label: (
                <span>
                  <SettingOutlined />
                  Spark配置
                </span>
              ),
              children: (
                <div>
                  <div style={{ marginBottom: 16 }}>
                    <Space>
                      <span>配置模式：</span>
                      <Select
                        value={configMode}
                        onChange={setConfigMode}
                        style={{ width: 120 }}
                      >
                        <Option value="visual">可视化配置</Option>
                        <Option value="json">JSON配置</Option>
                      </Select>
                      {configMode === 'visual' && (
                        <Button 
                          size="small" 
                          onClick={handleResetToDefault}
                          icon={<InfoCircleOutlined />}
                        >
                          重置默认值
                        </Button>
                      )}
                    </Space>
                  </div>

                  {configMode === 'visual' ? (
                    <div>
                      {/* Driver 配置 */}
                      <Card 
                        size="small" 
                        title={
                          <Space>
                            <SettingOutlined />
                            Driver 配置
                          </Space>
                        } 
                        style={{ marginBottom: 16 }}
                      >
                        <Row gutter={16}>
                          <Col span={8}>
                            <Form.Item
                              label={
                                <Space>
                                  <span>Driver内存</span>
                                  <Tooltip title="Driver进程的内存大小，建议2g-8g">
                                    <InfoCircleOutlined />
                                  </Tooltip>
                                </Space>
                              }
                              name="spark.driver.memory"
                            >
                              <Input placeholder="如：2g, 4g" />
                            </Form.Item>
                          </Col>
                          <Col span={8}>
                            <Form.Item
                              label={
                                <Space>
                                  <span>Driver核心数</span>
                                  <Tooltip title="Driver进程使用的CPU核心数，建议1-4">
                                    <InfoCircleOutlined />
                                  </Tooltip>
                                </Space>
                              }
                              name="spark.driver.cores"
                            >
                              <InputNumber min={1} max={16} style={{ width: '100%' }} />
                            </Form.Item>
                          </Col>
                          <Col span={8}>
                            <Form.Item
                              label={
                                <Space>
                                  <span>最大结果大小</span>
                                  <Tooltip title="Driver收集结果的最大大小">
                                    <InfoCircleOutlined />
                                  </Tooltip>
                                </Space>
                              }
                              name="spark.driver.maxResultSize"
                            >
                              <Input placeholder="如：1g, 2g" />
                            </Form.Item>
                          </Col>
                        </Row>
                      </Card>

                      {/* Executor 配置 */}
                      <Card 
                        size="small" 
                        title={
                          <Space>
                            <SettingOutlined />
                            Executor 配置
                          </Space>
                        } 
                        style={{ marginBottom: 16 }}
                      >
                        <Row gutter={16}>
                          <Col span={8}>
                            <Form.Item
                              label={
                                <Space>
                                  <span>Executor内存</span>
                                  <Tooltip title="每个Executor进程的内存大小，建议2g-8g">
                                    <InfoCircleOutlined />
                                  </Tooltip>
                                </Space>
                              }
                              name="spark.executor.memory"
                            >
                              <Input placeholder="如：4g, 8g" />
                            </Form.Item>
                          </Col>
                          <Col span={8}>
                            <Form.Item
                              label={
                                <Space>
                                  <span>Executor核心数</span>
                                  <Tooltip title="每个Executor使用的CPU核心数，建议2-8">
                                    <InfoCircleOutlined />
                                  </Tooltip>
                                </Space>
                              }
                              name="spark.executor.cores"
                            >
                              <InputNumber min={1} max={32} style={{ width: '100%' }} />
                            </Form.Item>
                          </Col>
                          <Col span={8}>
                            <Form.Item
                              label={
                                <Space>
                                  <span>Executor数量</span>
                                  <Tooltip title="Executor实例的数量，建议根据数据量调整">
                                    <InfoCircleOutlined />
                                  </Tooltip>
                                </Space>
                              }
                              name="spark.executor.instances"
                            >
                              <InputNumber min={1} max={1000} style={{ width: '100%' }} />
                            </Form.Item>
                          </Col>
                        </Row>
                        <Row gutter={16}>
                          <Col span={12}>
                            <Form.Item
                              label={
                                <Space>
                                  <span>内存使用比例</span>
                                  <Tooltip title="Executor内存中用于缓存和计算的比例">
                                    <InfoCircleOutlined />
                                  </Tooltip>
                                </Space>
                              }
                              name="spark.executor.memoryFraction"
                            >
                              <InputNumber min={0.1} max={1} step={0.1} style={{ width: '100%' }} />
                            </Form.Item>
                          </Col>
                        </Row>
                      </Card>

                      {/* 优化配置 */}
                      <Card 
                        size="small" 
                        title={
                          <Space>
                            <SettingOutlined />
                            优化配置
                          </Space>
                        }
                      >
                        <Row gutter={16}>
                          <Col span={12}>
                            <Form.Item
                              label={
                                <Space>
                                  <span>自适应查询执行</span>
                                  <Tooltip title="启用Spark SQL自适应查询执行优化">
                                    <InfoCircleOutlined />
                                  </Tooltip>
                                </Space>
                              }
                              name="spark.sql.adaptive.enabled"
                              valuePropName="checked"
                            >
                              <Switch />
                            </Form.Item>
                          </Col>
                          <Col span={12}>
                            <Form.Item
                              label={
                                <Space>
                                  <span>自动合并分区</span>
                                  <Tooltip title="自动合并小分区以提高性能">
                                    <InfoCircleOutlined />
                                  </Tooltip>
                                </Space>
                              }
                              name="spark.sql.adaptive.coalescePartitions.enabled"
                              valuePropName="checked"
                            >
                              <Switch />
                            </Form.Item>
                          </Col>
                        </Row>
                        <Row gutter={16}>
                          <Col span={8}>
                            <Form.Item
                              label="最小分区数"
                              name="spark.sql.adaptive.coalescePartitions.minPartitionNum"
                            >
                              <InputNumber min={1} style={{ width: '100%' }} />
                            </Form.Item>
                          </Col>
                          <Col span={8}>
                            <Form.Item
                              label="初始分区数"
                              name="spark.sql.adaptive.coalescePartitions.initialPartitionNum"
                            >
                              <InputNumber min={1} style={{ width: '100%' }} />
                            </Form.Item>
                          </Col>
                          <Col span={8}>
                            <Form.Item
                              label="序列化器"
                              name="spark.serializer"
                            >
                              <Select>
                                <Option value="org.apache.spark.serializer.KryoSerializer">
                                  KryoSerializer (推荐)
                                </Option>
                                <Option value="org.apache.spark.serializer.JavaSerializer">
                                  JavaSerializer
                                </Option>
                              </Select>
                            </Form.Item>
                          </Col>
                        </Row>
                        <Row gutter={16}>
                          <Col span={8}>
                            <Form.Item
                              label="Parquet过滤下推"
                              name="spark.sql.parquet.filterPushdown"
                              valuePropName="checked"
                            >
                              <Switch />
                            </Form.Item>
                          </Col>
                          <Col span={8}>
                            <Form.Item
                              label="Parquet模式合并"
                              name="spark.sql.parquet.mergeSchema"
                              valuePropName="checked"
                            >
                              <Switch />
                            </Form.Item>
                          </Col>
                          <Col span={8}>
                            <Form.Item
                              label="Hive Parquet转换"
                              name="spark.sql.hive.convertMetastoreParquet"
                              valuePropName="checked"
                            >
                              <Switch />
                            </Form.Item>
                          </Col>
                        </Row>
                      </Card>
                    </div>
                  ) : (
                    <Form.Item
                      label={
                        <Space>
                          <CodeOutlined />
                          JSON配置
                        </Space>
                      }
                      name="conf"
                      rules={[
                        {
                          validator: (_, value) => {
                            if (!value) return Promise.resolve();
                            try {
                              JSON.parse(value);
                              return Promise.resolve();
                            } catch (error) {
                              return Promise.reject(new Error('请输入有效的JSON格式配置'));
                            }
                          },
                        },
                      ]}
                    >
                      <TextArea
                        placeholder='请输入JSON格式的配置信息，如：{"spark.driver.memory": "4g"}'
                        rows={12}
                        showCount
                      />
                    </Form.Item>
                  )}
                </div>
              ),
            },
          ]}
        />
      </Form>
    </Modal>
  );
};

export default CreateApplicationModal; 