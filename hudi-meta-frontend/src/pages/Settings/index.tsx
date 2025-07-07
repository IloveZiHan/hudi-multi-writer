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
} from 'antd';
import {
  DatabaseOutlined,
  SyncOutlined,
  SettingOutlined,
  SaveOutlined,
  ReloadOutlined,
  BellOutlined,
  SecurityScanOutlined,
} from '@ant-design/icons';

const { Option } = Select;
const { TextArea } = Input;

/**
 * 系统设置组件
 * 提供系统配置管理功能
 */
const Settings: React.FC = () => {
  const [form] = Form.useForm();
  const [loading, setLoading] = useState(false);

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
    <div>
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