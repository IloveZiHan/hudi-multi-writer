import React from 'react';
import {
  Modal,
  Descriptions,
  Typography,
  Space,
  Tag,
  Card,
  Empty,
} from 'antd';
import { EyeOutlined, AppstoreOutlined } from '@ant-design/icons';
import dayjs from 'dayjs';
import { ApplicationDTO } from '@services/applicationApi';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { vscDarkPlus } from 'react-syntax-highlighter/dist/esm/styles/prism';

const { Text, Title } = Typography;

interface ApplicationDetailModalProps {
  visible: boolean;
  record: ApplicationDTO | null;
  onCancel: () => void;
}

/**
 * 应用程序详情模态框组件
 */
const ApplicationDetailModal: React.FC<ApplicationDetailModalProps> = ({
  visible,
  record,
  onCancel,
}) => {
  if (!record) {
    return null;
  }

  // 格式化配置信息
  const formatConfig = (conf?: string) => {
    if (!conf) return null;
    try {
      const parsed = JSON.parse(conf);
      return JSON.stringify(parsed, null, 2);
    } catch (error) {
      return conf;
    }
  };

  return (
    <Modal
      title={
        <Space>
          <EyeOutlined />
          应用程序详情 - {record.name}
        </Space>
      }
      open={visible}
      onCancel={onCancel}
      footer={null}
      width={800}
      destroyOnClose
    >
      <div style={{ maxHeight: '70vh', overflowY: 'auto' }}>
        {/* 基本信息 */}
        <Card title="基本信息" style={{ marginBottom: 16 }}>
          <Descriptions bordered column={2}>
            <Descriptions.Item label="应用ID" span={1}>
              <Tag color="blue">{record.id}</Tag>
            </Descriptions.Item>
            <Descriptions.Item label="应用名称" span={1}>
              <Text strong>{record.name}</Text>
            </Descriptions.Item>
            <Descriptions.Item label="描述信息" span={2}>
              {record.description ? (
                <Text>{record.description}</Text>
              ) : (
                <Text type="secondary">暂无描述</Text>
              )}
            </Descriptions.Item>
            <Descriptions.Item label="创建时间" span={1}>
              {dayjs(record.createTime).format('YYYY-MM-DD HH:mm:ss')}
            </Descriptions.Item>
            <Descriptions.Item label="更新时间" span={1}>
              {dayjs(record.updateTime).format('YYYY-MM-DD HH:mm:ss')}
            </Descriptions.Item>
          </Descriptions>
        </Card>

        {/* 配置信息 */}
        <Card title="配置信息">
          {record.conf ? (
            <div>
              <div style={{ marginBottom: 8 }}>
                <Text type="secondary">JSON配置：</Text>
              </div>
              <SyntaxHighlighter
                language="json"
                style={vscDarkPlus}
                customStyle={{
                  margin: 0,
                  borderRadius: 6,
                  fontSize: 13,
                  maxHeight: 400,
                  overflow: 'auto'
                }}
              >
                {formatConfig(record.conf) || record.conf}
              </SyntaxHighlighter>
            </div>
          ) : (
            <Empty 
              image={Empty.PRESENTED_IMAGE_SIMPLE}
              description="暂无配置信息"
            />
          )}
        </Card>
      </div>
    </Modal>
  );
};

export default ApplicationDetailModal; 