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
} from 'antd';
import { EditOutlined, DatabaseOutlined } from '@ant-design/icons';
import { useRequest } from 'ahooks';
import tableApiService from '@services/tableApi';
import { UpdateTableRequest, TableStatus, MetaTableDTO } from '@types/api';

const { Option } = Select;
const { TextArea } = Input;

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

  // 当record变化时，重置表单数据
  useEffect(() => {
    if (visible && record) {
      form.setFieldsValue({
        schema: record.schema,
        status: record.status,
        isPartitioned: record.isPartitioned,
        partitionExpr: record.partitionExpr,
        tags: record.tags,
        description: record.description,
        sourceDb: record.sourceDb,
        sourceTable: record.sourceTable,
        targetDb: record.targetDb,
        targetTable: record.targetTable,
      });
      setIsPartitioned(record.isPartitioned);
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
    form.resetFields();
    setIsPartitioned(false);
    onCancel();
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
      onCancel={handleCancel}
      footer={[
        <Button key="cancel" onClick={handleCancel}>
          取消
        </Button>,
        <Button key="submit" type="primary" loading={loading} onClick={handleSubmit}>
          更新
        </Button>,
      ]}
      width={800}
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

        <Row gutter={16}>
          <Col span={12}>
            <Form.Item label="创建人">
              <Input value={record?.creator || '未知'} disabled />
            </Form.Item>
          </Col>
          <Col span={12}>
            <Form.Item label="更新人">
              <Input value={record?.updater || '未知'} disabled />
            </Form.Item>
          </Col>
        </Row>
      </Form>
    </Modal>
  );
};

export default EditTableModal; 