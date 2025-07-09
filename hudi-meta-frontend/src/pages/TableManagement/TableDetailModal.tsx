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
  Table,
} from 'antd';
import {
  EyeOutlined,
  DatabaseOutlined,
  InfoCircleOutlined,
  PartitionOutlined,
  TagsOutlined,
  CalendarOutlined,
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

  // 解析表结构 JSON
  const parseTableSchema = (schema: string) => {
    try {
      const parsed = JSON.parse(schema);
      if (parsed.type === 'struct' && parsed.fields) {
        return parsed.fields.map((field: any, index: number) => ({
          key: index,
          name: field.name,
          type: field.type,
          nullable: field.nullable,
          comment: field.metadata?.comment || '无注释',
        }));
      }
      return [];
    } catch (error) {
      console.error('解析表结构失败:', error);
      return [];
    }
  };

  // 表结构表格列定义
  const schemaColumns = [
    {
      title: '字段名',
      dataIndex: 'name',
      key: 'name',
      width: 150,
      render: (text: string) => <Text strong>{text}</Text>,
    },
    {
      title: '数据类型',
      dataIndex: 'type',
      key: 'type',
      width: 120,
      render: (text: string) => <Tag color="blue">{text}</Tag>,
    },
    {
      title: '是否可为空',
      dataIndex: 'nullable',
      key: 'nullable',
      width: 100,
      render: (nullable: boolean) => (
        <Badge
          status={nullable ? 'warning' : 'success'}
          text={nullable ? '可为空' : '不可为空'}
        />
      ),
    },
    {
      title: '注释',
      dataIndex: 'comment',
      key: 'comment',
      render: (text: string) => (
        text === '无注释' ? (
          <Text type="secondary">{text}</Text>
        ) : (
          <Text>{text}</Text>
        )
      ),
    },
  ];

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
          <Table
            columns={schemaColumns}
            dataSource={parseTableSchema(record.schema)}
            pagination={false}
            size="small"
            bordered
            style={{ marginTop: 12 }}
            scroll={{ x: 600 }}
            locale={{
              emptyText: '暂无字段信息',
            }}
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