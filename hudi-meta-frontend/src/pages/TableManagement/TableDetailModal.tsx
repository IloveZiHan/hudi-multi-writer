import React from 'react';
import {
  Modal,
  Form,
  Input,
  Tag,
  Badge,
  Row,
  Col,
  Space,
  Divider,
  Typography,
  Card,
  Descriptions,
} from 'antd';
import {
  EyeOutlined,
  DatabaseOutlined,
  InfoCircleOutlined,
  PartitionOutlined,
  TagsOutlined,
  CalendarOutlined,
  UserOutlined,
} from '@ant-design/icons';
import dayjs from 'dayjs';
import { MetaTableDTO, TableStatus, TableStatusLabels, TableStatusColors } from '@types/api';

const { TextArea } = Input;
const { Text, Title } = Typography;

interface TableDetailModalProps {
  visible: boolean;
  record: MetaTableDTO | null;
  onCancel: () => void;
}

/**
 * 表详情模态框组件
 */
const TableDetailModal: React.FC<TableDetailModalProps> = ({
  visible,
  record,
  onCancel,
}) => {
  if (!record) return null;

  // 格式化标签
  const formatTags = (tags?: string) => {
    if (!tags) return [];
    return tags.split(',').map(tag => tag.trim()).filter(Boolean);
  };

  return (
    <Modal
      title={
        <Space>
          <EyeOutlined />
          表详情 - {record.id}
        </Space>
      }
      open={visible}
      onCancel={onCancel}
      footer={null}
      width={900}
      destroyOnClose
    >
      <div style={{ maxHeight: '70vh', overflowY: 'auto' }}>
        {/* 基本信息 */}
        <Card size="small" style={{ marginBottom: 16 }}>
          <Descriptions
            title={
              <Space>
                <DatabaseOutlined />
                基本信息
              </Space>
            }
            bordered
            column={2}
            size="small"
          >
            <Descriptions.Item label="表ID" span={2}>
              <Text strong>{record.id}</Text>
            </Descriptions.Item>
            <Descriptions.Item label="状态">
              <Tag color={TableStatusColors[record.status as TableStatus]}>
                {TableStatusLabels[record.status as TableStatus]}
              </Tag>
            </Descriptions.Item>
            <Descriptions.Item label="分区表">
              <Badge
                status={record.isPartitioned ? 'success' : 'default'}
                text={record.isPartitioned ? '是' : '否'}
              />
            </Descriptions.Item>
            {record.isPartitioned && record.partitionExpr && (
              <Descriptions.Item label="分区表达式" span={2}>
                <Text code>{record.partitionExpr}</Text>
              </Descriptions.Item>
            )}
          </Descriptions>
        </Card>

        {/* 表结构 */}
        <Card size="small" style={{ marginBottom: 16 }}>
          <Title level={5}>
            <Space>
              <InfoCircleOutlined />
              表结构
            </Space>
          </Title>
          <TextArea
            value={record.schema}
            rows={8}
            style={{ 
              fontFamily: 'monospace',
              backgroundColor: '#f5f5f5',
              border: '1px solid #d9d9d9',
            }}
            readOnly
          />
        </Card>

        {/* 数据源配置 */}
        <Card size="small" style={{ marginBottom: 16 }}>
          <Descriptions
            title={
              <Space>
                <DatabaseOutlined />
                数据源配置
              </Space>
            }
            bordered
            column={2}
            size="small"
          >
            <Descriptions.Item label="源数据库">
              <Text>{record.sourceDb || '-'}</Text>
            </Descriptions.Item>
            <Descriptions.Item label="源表名">
              <Text>{record.sourceTable || '-'}</Text>
            </Descriptions.Item>
            <Descriptions.Item label="目标数据库">
              <Text>{record.targetDb || '-'}</Text>
            </Descriptions.Item>
            <Descriptions.Item label="目标表名">
              <Text>{record.targetTable || '-'}</Text>
            </Descriptions.Item>
          </Descriptions>
        </Card>

        {/* 标签和描述 */}
        <Card size="small" style={{ marginBottom: 16 }}>
          <Descriptions
            title={
              <Space>
                <TagsOutlined />
                附加信息
              </Space>
            }
            bordered
            column={1}
            size="small"
          >
            <Descriptions.Item label="标签">
              {formatTags(record.tags).length > 0 ? (
                <Space wrap>
                  {formatTags(record.tags).map(tag => (
                    <Tag key={tag} color="blue">
                      {tag}
                    </Tag>
                  ))}
                </Space>
              ) : (
                <Text type="secondary">无标签</Text>
              )}
            </Descriptions.Item>
            <Descriptions.Item label="描述">
              <Text>{record.description || '无描述'}</Text>
            </Descriptions.Item>
          </Descriptions>
        </Card>

        {/* 元数据信息 */}
        <Card size="small">
          <Descriptions
            title={
              <Space>
                <CalendarOutlined />
                元数据信息
              </Space>
            }
            bordered
            column={2}
            size="small"
          >
            <Descriptions.Item label="创建时间">
              <Text>{dayjs(record.createdTime).format('YYYY-MM-DD HH:mm:ss')}</Text>
            </Descriptions.Item>
            <Descriptions.Item label="更新时间">
              <Text>{dayjs(record.updatedTime).format('YYYY-MM-DD HH:mm:ss')}</Text>
            </Descriptions.Item>
            <Descriptions.Item label="创建人">
              <Space>
                <UserOutlined />
                <Text>{record.creator || '未知'}</Text>
              </Space>
            </Descriptions.Item>
            <Descriptions.Item label="更新人">
              <Space>
                <UserOutlined />
                <Text>{record.updater || '未知'}</Text>
              </Space>
            </Descriptions.Item>
          </Descriptions>
        </Card>

        {/* 统计信息 */}
        <Card size="small" style={{ marginTop: 16 }}>
          <Descriptions
            title={
              <Space>
                <InfoCircleOutlined />
                统计信息
              </Space>
            }
            bordered
            column={2}
            size="small"
          >
            <Descriptions.Item label="存在天数">
              <Text>
                {dayjs().diff(dayjs(record.createdTime), 'day')} 天
              </Text>
            </Descriptions.Item>
            <Descriptions.Item label="最后更新">
              <Text>
                {dayjs().diff(dayjs(record.updatedTime), 'hour')} 小时前
              </Text>
            </Descriptions.Item>
          </Descriptions>
        </Card>
      </div>
    </Modal>
  );
};

export default TableDetailModal; 