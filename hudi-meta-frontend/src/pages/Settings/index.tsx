import React, { useState } from 'react';
import {
  Card,
  Form,
  Input,
  Button,
  Switch,
  Select,
  InputNumber,
  Divider,
  Space,
  message,
  Row,
  Col,
  Alert,
  Tabs,
  Modal,
  Table,
  Tag,
  Progress,
  Descriptions,
  Typography,
} from 'antd';
import {
  DatabaseOutlined,
  SyncOutlined,
  SettingOutlined,
  SaveOutlined,
  ReloadOutlined,
  BellOutlined,
  SecurityScanOutlined,
  ThunderboltOutlined,
  CheckCircleOutlined,
  ExclamationCircleOutlined,
  DeleteOutlined,
} from '@ant-design/icons';
import { useRequest } from 'ahooks';
import tableApiService from '@services/tableApi';
import './index.less';

const { Option } = Select;
const { TextArea } = Input;
const { Title, Text } = Typography;

/**
 * 系统设置组件
 * 提供系统配置管理功能
 */
const Settings: React.FC = () => {
  const [form] = Form.useForm();
  const [loading, setLoading] = useState(false);
  const [initLoading, setInitLoading] = useState(false);
  const [checkLoading, setCheckLoading] = useState(false);
  const [dropLoading, setDropLoading] = useState(false);
  const [metaTablesStatus, setMetaTablesStatus] = useState<any>(null);

  // 检查元数据表是否存在
  const { data: metaTablesExist, run: checkMetaTablesExist } = useRequest(
    () => tableApiService.checkMetaTablesExist(),
    {
      manual: true,
      onSuccess: (exists) => {
        console.log('元数据表是否存在:', exists);
      },
      onError: (error) => {
        message.error(`检查元数据表失败: ${error.message}`);
      },
    }
  );

  // 获取元数据表状态
  const { data: metaTablesStatusData, run: getMetaTablesStatus } = useRequest(
    () => tableApiService.getMetaTablesStatus(),
    {
      manual: true,
      onSuccess: (status) => {
        setMetaTablesStatus(status);
      },
      onError: (error) => {
        message.error(`获取元数据表状态失败: ${error.message}`);
      },
    }
  );

  // 处理表单提交
  const handleSubmit = async (values: any) => {
    setLoading(true);
    try {
      // 模拟API调用
      await new Promise(resolve => setTimeout(resolve, 1000));
      message.success('设置保存成功！');
      console.log('保存的设置:', values);
    } catch (error) {
      message.error('保存失败，请重试');
    } finally {
      setLoading(false);
    }
  };

  // 重置表单
  const handleReset = () => {
    form.resetFields();
    message.info('设置已重置');
  };

  // 初始化Hudi元数据表
  const handleInitializeMetaTables = async () => {
    Modal.confirm({
      title: '确认初始化',
      icon: <ExclamationCircleOutlined />,
      content: '确定要初始化Hudi元数据表吗？这将创建必要的系统表结构。',
      okText: '确定',
      cancelText: '取消',
      onOk: async () => {
        setInitLoading(true);
        try {
          await tableApiService.initializeHudiMetaTables();
          message.success('Hudi元数据表初始化成功！');
          // 刷新状态
          checkMetaTablesExist();
          getMetaTablesStatus();
        } catch (error) {
          message.error(`初始化失败: ${error}`);
        } finally {
          setInitLoading(false);
        }
      },
    });
  };

  // 检查元数据表状态
  const handleCheckMetaTables = async () => {
    setCheckLoading(true);
    try {
      await Promise.all([checkMetaTablesExist(), getMetaTablesStatus()]);
      message.success('检查完成');
    } catch (error) {
      message.error(`检查失败: ${error}`);
    } finally {
      setCheckLoading(false);
    }
  };

  // 删除所有元数据表
  const handleDropMetaTables = async () => {
    Modal.confirm({
      title: '危险操作',
      icon: <ExclamationCircleOutlined />,
      content: (
        <div>
          <p style={{ color: '#ff4d4f', fontWeight: 'bold' }}>
            警告：此操作将删除所有Hudi元数据表！
          </p>
          <p>这将导致：</p>
          <ul>
            <li>所有表配置信息丢失</li>
            <li>系统无法正常工作</li>
            <li>需要重新初始化才能恢复</li>
          </ul>
          <p>确定要继续吗？</p>
        </div>
      ),
      okText: '确定删除',
      cancelText: '取消',
      okType: 'danger',
      onOk: async () => {
        setDropLoading(true);
        try {
          await tableApiService.dropAllMetaTables();
          message.success('元数据表删除成功');
          // 刷新状态
          checkMetaTablesExist();
          getMetaTablesStatus();
        } catch (error) {
          message.error(`删除失败: ${error}`);
        } finally {
          setDropLoading(false);
        }
      },
    });
  };

  // 数据库初始化选项卡
  const DatabaseInitConfig = () => (
    <Space direction="vertical" style={{ width: '100%' }}>
      <Alert
        message="数据库初始化"
        description="管理Hudi元数据表的创建、检查和维护"
        type="info"
        showIcon
        icon={<DatabaseOutlined />}
      />

      {/* 当前状态 */}
      <Card title="当前状态" size="small">
        <Descriptions column={2} bordered size="small">
          <Descriptions.Item label="元数据表状态">
            {metaTablesExist === true ? (
              <Tag color="green" icon={<CheckCircleOutlined />}>
                已创建
              </Tag>
            ) : metaTablesExist === false ? (
              <Tag color="red" icon={<ExclamationCircleOutlined />}>
                未创建
              </Tag>
            ) : (
              <Tag color="default">未知</Tag>
            )}
          </Descriptions.Item>
          <Descriptions.Item label="系统状态">
            {metaTablesExist === true ? (
              <Tag color="green">正常</Tag>
            ) : (
              <Tag color="orange">需要初始化</Tag>
            )}
          </Descriptions.Item>
        </Descriptions>
      </Card>

      {/* 元数据表详情 */}
      {metaTablesStatus && (
        <Card title="元数据表详情" size="small">
          <Table
            size="small"
            dataSource={metaTablesStatus.tables || []}
            columns={[
              {
                title: '表名',
                dataIndex: 'name',
                key: 'name',
              },
              {
                title: '状态',
                dataIndex: 'status',
                key: 'status',
                render: (status: string) => (
                  <Tag color={status === 'EXISTS' ? 'green' : 'red'}>
                    {status === 'EXISTS' ? '存在' : '不存在'}
                  </Tag>
                ),
              },
              {
                title: '记录数',
                dataIndex: 'count',
                key: 'count',
                render: (count: number) => count?.toLocaleString() || 0,
              },
              {
                title: '大小',
                dataIndex: 'size',
                key: 'size',
              },
              {
                title: '最后更新',
                dataIndex: 'lastModified',
                key: 'lastModified',
              },
            ]}
            pagination={false}
          />
        </Card>
      )}

      {/* 操作按钮 */}
      <Card title="操作" size="small">
        <Space direction="vertical" style={{ width: '100%' }}>
          <Row gutter={16}>
            <Col span={8}>
              <Button
                type="primary"
                icon={<ThunderboltOutlined />}
                onClick={handleInitializeMetaTables}
                loading={initLoading}
                block
                disabled={metaTablesExist === true}
              >
                初始化元数据表
              </Button>
            </Col>
            <Col span={8}>
              <Button
                icon={<ReloadOutlined />}
                onClick={handleCheckMetaTables}
                loading={checkLoading}
                block
              >
                检查状态
              </Button>
            </Col>
            <Col span={8}>
              <Button
                danger
                icon={<DeleteOutlined />}
                onClick={handleDropMetaTables}
                loading={dropLoading}
                block
                disabled={metaTablesExist === false}
              >
                删除所有表
              </Button>
            </Col>
          </Row>

          {/* 说明信息 */}
          <Alert
            message="操作说明"
            description={
              <ul style={{ marginBottom: 0, paddingLeft: 20 }}>
                <li>初始化元数据表：创建Hudi系统必需的元数据表结构</li>
                <li>检查状态：检查当前元数据表的存在状态和详细信息</li>
                <li>删除所有表：危险操作，将删除所有元数据表</li>
              </ul>
            }
            type="info"
            showIcon
          />
        </Space>
      </Card>
    </Space>
  );

  // 数据库配置选项卡
  const DatabaseConfig = () => (
    <Space direction="vertical" style={{ width: '100%' }}>
      <Alert
        message="数据库配置"
        description="配置Hudi元数据存储的数据库连接信息"
        type="info"
        showIcon
        icon={<DatabaseOutlined />}
      />
      
      <Row gutter={16}>
        <Col span={12}>
          <Form.Item
            label="数据库类型"
            name="dbType"
            rules={[{ required: true, message: '请选择数据库类型' }]}
          >
            <Select placeholder="选择数据库类型">
              <Option value="mysql">MySQL</Option>
              <Option value="postgresql">PostgreSQL</Option>
              <Option value="oracle">Oracle</Option>
              <Option value="sqlserver">SQL Server</Option>
            </Select>
          </Form.Item>
        </Col>
        <Col span={12}>
          <Form.Item
            label="数据库主机"
            name="dbHost"
            rules={[{ required: true, message: '请输入数据库主机地址' }]}
          >
            <Input placeholder="localhost" />
          </Form.Item>
        </Col>
      </Row>

      <Row gutter={16}>
        <Col span={12}>
          <Form.Item
            label="端口"
            name="dbPort"
            rules={[{ required: true, message: '请输入端口号' }]}
          >
            <InputNumber min={1} max={65535} placeholder="3306" style={{ width: '100%' }} />
          </Form.Item>
        </Col>
        <Col span={12}>
          <Form.Item
            label="数据库名"
            name="dbName"
            rules={[{ required: true, message: '请输入数据库名' }]}
          >
            <Input placeholder="hudi_meta" />
          </Form.Item>
        </Col>
      </Row>

      <Row gutter={16}>
        <Col span={12}>
          <Form.Item
            label="用户名"
            name="dbUsername"
            rules={[{ required: true, message: '请输入用户名' }]}
          >
            <Input placeholder="root" />
          </Form.Item>
        </Col>
        <Col span={12}>
          <Form.Item
            label="密码"
            name="dbPassword"
            rules={[{ required: true, message: '请输入密码' }]}
          >
            <Input.Password placeholder="请输入密码" />
          </Form.Item>
        </Col>
      </Row>

      <Form.Item label="连接池大小" name="connectionPoolSize">
        <InputNumber min={1} max={100} placeholder="10" style={{ width: '100%' }} />
      </Form.Item>

      <Form.Item label="连接超时(秒)" name="connectionTimeout">
        <InputNumber min={5} max={300} placeholder="30" style={{ width: '100%' }} />
      </Form.Item>
    </Space>
  );

  // 同步配置选项卡
  const SyncConfig = () => (
    <Space direction="vertical" style={{ width: '100%' }}>
      <Alert
        message="同步配置"
        description="配置Hudi表的同步策略和参数"
        type="info"
        showIcon
        icon={<SyncOutlined />}
      />

      <Row gutter={16}>
        <Col span={12}>
          <Form.Item
            label="同步间隔(分钟)"
            name="syncInterval"
            rules={[{ required: true, message: '请输入同步间隔' }]}
          >
            <InputNumber min={1} max={1440} placeholder="5" style={{ width: '100%' }} />
          </Form.Item>
        </Col>
        <Col span={12}>
          <Form.Item
            label="批处理大小"
            name="batchSize"
            rules={[{ required: true, message: '请输入批处理大小' }]}
          >
            <InputNumber min={100} max={10000} placeholder="1000" style={{ width: '100%' }} />
          </Form.Item>
        </Col>
      </Row>

      <Row gutter={16}>
        <Col span={12}>
          <Form.Item label="启用自动同步" name="autoSync" valuePropName="checked">
            <Switch />
          </Form.Item>
        </Col>
        <Col span={12}>
          <Form.Item label="失败重试次数" name="retryCount">
            <InputNumber min={0} max={10} placeholder="3" style={{ width: '100%' }} />
          </Form.Item>
        </Col>
      </Row>

      <Form.Item label="同步模式" name="syncMode">
        <Select placeholder="选择同步模式">
          <Option value="incremental">增量同步</Option>
          <Option value="full">全量同步</Option>
          <Option value="hybrid">混合模式</Option>
        </Select>
      </Form.Item>

      <Form.Item label="排除表模式" name="excludePattern">
        <TextArea 
          rows={3} 
          placeholder="输入要排除的表名模式，一行一个，支持正则表达式"
        />
      </Form.Item>
    </Space>
  );

  // 系统配置选项卡
  const SystemConfig = () => (
    <Space direction="vertical" style={{ width: '100%' }}>
      <Alert
        message="系统配置"
        description="配置系统运行参数和监控告警"
        type="info"
        showIcon
        icon={<SettingOutlined />}
      />

      <Row gutter={16}>
        <Col span={12}>
          <Form.Item label="日志级别" name="logLevel">
            <Select placeholder="选择日志级别">
              <Option value="DEBUG">DEBUG</Option>
              <Option value="INFO">INFO</Option>
              <Option value="WARN">WARN</Option>
              <Option value="ERROR">ERROR</Option>
            </Select>
          </Form.Item>
        </Col>
        <Col span={12}>
          <Form.Item label="监控端口" name="monitorPort">
            <InputNumber min={1000} max={65535} placeholder="9090" style={{ width: '100%' }} />
          </Form.Item>
        </Col>
      </Row>

      <Row gutter={16}>
        <Col span={12}>
          <Form.Item label="启用监控" name="enableMonitor" valuePropName="checked">
            <Switch />
          </Form.Item>
        </Col>
        <Col span={12}>
          <Form.Item label="启用告警" name="enableAlert" valuePropName="checked">
            <Switch />
          </Form.Item>
        </Col>
      </Row>

      <Form.Item label="告警邮箱" name="alertEmail">
        <Input placeholder="admin@example.com" />
      </Form.Item>

      <Form.Item label="系统备注" name="systemNote">
        <TextArea rows={4} placeholder="输入系统备注信息" />
      </Form.Item>
    </Space>
  );

  // 选项卡配置
  const tabItems = [
    {
      key: 'database-init',
      label: (
        <span>
          <ThunderboltOutlined />
          数据库初始化
        </span>
      ),
      children: <DatabaseInitConfig />,
    },
    {
      key: 'database',
      label: (
        <span>
          <DatabaseOutlined />
          数据库配置
        </span>
      ),
      children: <DatabaseConfig />,
    },
    {
      key: 'sync',
      label: (
        <span>
          <SyncOutlined />
          同步配置
        </span>
      ),
      children: <SyncConfig />,
    },
    {
      key: 'system',
      label: (
        <span>
          <SettingOutlined />
          系统配置
        </span>
      ),
      children: <SystemConfig />,
    },
  ];

  return (
    <div className="settings">
      {/* 页面标题 */}
      <div className="page-header">
        <Title level={2}>
          <SettingOutlined />
          系统设置
        </Title>
        <Text type="secondary">
          配置系统运行参数、数据库连接信息和监控告警设置
        </Text>
      </div>

      <Card
        title={
          <Space>
            <SettingOutlined />
            系统设置
          </Space>
        }
        extra={
          <Space>
            <Button
              icon={<ReloadOutlined />}
              onClick={handleReset}
            >
              重置
            </Button>
            <Button
              type="primary"
              icon={<SaveOutlined />}
              loading={loading}
              onClick={() => form.submit()}
            >
              保存设置
            </Button>
          </Space>
        }
      >
        <Form
          form={form}
          layout="vertical"
          onFinish={handleSubmit}
          initialValues={{
            dbType: 'mysql',
            dbHost: 'localhost',
            dbPort: 3306,
            dbName: 'hudi_meta',
            dbUsername: 'root',
            connectionPoolSize: 10,
            connectionTimeout: 30,
            syncInterval: 5,
            batchSize: 1000,
            autoSync: true,
            retryCount: 3,
            syncMode: 'incremental',
            logLevel: 'INFO',
            monitorPort: 9090,
            enableMonitor: true,
            enableAlert: false,
          }}
        >
          <Tabs items={tabItems} />
        </Form>
      </Card>
    </div>
  );
};

export default Settings; 