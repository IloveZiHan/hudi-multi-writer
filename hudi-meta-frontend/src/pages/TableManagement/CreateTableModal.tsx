import React, { useState } from 'react';
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
} from 'antd';
import { PlusOutlined, DatabaseOutlined } from '@ant-design/icons';
import { useRequest } from 'ahooks';
import tableApiService from '@services/tableApi';
import { CreateTableRequest, TableStatus, SupportedDbTypes } from '@types/api';

const { Option } = Select;
const { TextArea } = Input;

interface CreateTableModalProps {
  visible: boolean;
  onCancel: () => void;
  onSuccess: () => void;
}

/**
 * 创建表模态框组件
 */
const CreateTableModal: React.FC<CreateTableModalProps> = ({
  visible,
  onCancel,
  onSuccess,
}) => {
  const [form] = Form.useForm();
  const [isPartitioned, setIsPartitioned] = useState(false);

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
      const values = await form.validateFields();
      await createTable(values);
    } catch (error) {
      // 表单验证失败
    }
  };

  // 处理取消
  const handleCancel = () => {
    form.resetFields();
    setIsPartitioned(false);
    onCancel();
  };

  return (
    <Modal
      title={
        <Space>
          <PlusOutlined />
          创建表
        </Space>
      }
      open={visible}
      onCancel={handleCancel}
      footer={[
        <Button key="cancel" onClick={handleCancel}>
          取消
        </Button>,
        <Button key="submit" type="primary" loading={loading} onClick={handleSubmit}>
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
          status: TableStatus.OFFLINE,
          isPartitioned: false,
        }}
      >
        <Divider orientation="left">
          <DatabaseOutlined /> 基本信息
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
              <Input placeholder="请输入表ID" />
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
          label="表结构"
          name="schema"
          rules={[{ required: true, message: '请输入表结构' }]}
        >
          <TextArea
            rows={6}
            placeholder="请输入表结构JSON或DDL语句"
            style={{ fontFamily: 'monospace' }}
          />
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

        <Row gutter={16}>
          <Col span={12}>
            <Form.Item
              label="目标数据库"
              name="targetDb"
              rules={[{ required: true, message: '请输入目标数据库' }]}
            >
              <Input placeholder="请输入目标数据库名称" />
            </Form.Item>
          </Col>
          <Col span={12}>
            <Form.Item
              label="目标表名"
              name="targetTable"
              rules={[{ required: true, message: '请输入目标表名' }]}
            >
              <Input placeholder="请输入目标表名称" />
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
      </Form>
    </Modal>
  );
};

export default CreateTableModal; 