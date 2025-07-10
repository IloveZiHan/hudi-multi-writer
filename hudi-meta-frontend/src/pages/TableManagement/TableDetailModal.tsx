import React, { useState } from 'react';
import {
  Modal,
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
  Tabs,
} from 'antd';
import {
  EyeOutlined,
  DatabaseOutlined,
  InfoCircleOutlined,
  PartitionOutlined,
  TagsOutlined,
  CalendarOutlined,
  TableOutlined,
  SettingOutlined,
  FileTextOutlined,
} from '@ant-design/icons';
import dayjs from 'dayjs';
import { MetaTableDTO, TableStatus, TableStatusLabels, TableStatusColors } from '../../types/api';

const { TextArea } = Input;
const { Text, Title } = Typography;
const { TabPane } = Tabs;

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
  const [activeTab, setActiveTab] = useState('basic'); // 当前活动Tab

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

  // 解析Hoodie配置JSON
  const parseHoodieConfig = (config: string) => {
    try {
      const parsed = JSON.parse(config);
      if (typeof parsed === 'object' && parsed !== null) {
        return Object.entries(parsed).map(([key, value], index) => ({
          key: index,
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

  // Hoodie配置表格列定义
  const hoodieConfigColumns = [
    {
      title: '配置项',
      dataIndex: 'configKey',
      key: 'configKey',
      width: '50%',
      render: (text: string) => <Text code>{text}</Text>,
    },
    {
      title: '配置值',
      dataIndex: 'configValue',
      key: 'configValue',
      render: (text: string) => <Text>{text}</Text>,
    },
  ];

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
          <Card size="small">
            <Descriptions column={1} size="small" bordered>
              <Descriptions.Item label="表ID">
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
                <Descriptions.Item label="分区表达式">
                  <Text code>{record.partitionExpr}</Text>
                </Descriptions.Item>
              )}
            </Descriptions>
          </Card>
        </Col>
        <Col span={12}>
          <Card size="small">
            <Descriptions column={1} size="small" bordered>
              <Descriptions.Item label="创建时间">
                <Text>{dayjs(record.createdTime).format('YYYY-MM-DD HH:mm:ss')}</Text>
              </Descriptions.Item>
              <Descriptions.Item label="更新时间">
                <Text>{dayjs(record.updatedTime).format('YYYY-MM-DD HH:mm:ss')}</Text>
              </Descriptions.Item>
              <Descriptions.Item label="创建者">
                <Text>{record.creator || '未知'}</Text>
              </Descriptions.Item>
              <Descriptions.Item label="更新者">
                <Text>{record.updater || '未知'}</Text>
              </Descriptions.Item>
            </Descriptions>
          </Card>
        </Col>
      </Row>

      <Divider orientation="left">
        <Space>
          <DatabaseOutlined />
          数据源配置
        </Space>
      </Divider>

      <Card size="small">
        <Descriptions bordered column={2} size="small">
          <Descriptions.Item label="数据库类型">
            <Text>{record.sourceDb ? '数据库' : '未知'}</Text>
          </Descriptions.Item>
          <Descriptions.Item label="目标数据库">
            <Text>{record.targetDb || '未设置'}</Text>
          </Descriptions.Item>
          <Descriptions.Item label="源数据库">
            <Text>{record.sourceDb || '未设置'}</Text>
          </Descriptions.Item>
          <Descriptions.Item label="目标表名">
            <Text>{record.targetTable || '未设置'}</Text>
          </Descriptions.Item>
          <Descriptions.Item label="源表名">
            <Text>{record.sourceTable || '未设置'}</Text>
          </Descriptions.Item>
          <Descriptions.Item label="分区配置">
            <Text>{record.isPartitioned ? '已启用' : '未启用'}</Text>
          </Descriptions.Item>
        </Descriptions>
      </Card>
    </>
  );

  // 渲染表结构Tab
  const renderSchemaTab = () => (
    <>
      <Card size="small">
        <Tabs defaultActiveKey="table" type="card">
          <TabPane tab="表结构视图" key="table">
            <Table
              columns={schemaColumns}
              dataSource={parseTableSchema(record.schema)}
              pagination={false}
              size="small"
              bordered
              scroll={{ x: 600, y: 400 }}
              locale={{
                emptyText: '暂无字段信息',
              }}
            />
          </TabPane>
          
          <TabPane tab="JSON Schema预览" key="json">
            <div style={{ 
              border: '1px solid #d9d9d9', 
              borderRadius: '6px',
              padding: '16px',
              minHeight: '400px',
              background: '#f6f6f6'
            }}>
              <pre style={{ 
                margin: 0, 
                fontFamily: 'monospace',
                fontSize: '12px',
                whiteSpace: 'pre-wrap',
                wordBreak: 'break-word'
              }}>
                {JSON.stringify(JSON.parse(record.schema), null, 2)}
              </pre>
            </div>
          </TabPane>
        </Tabs>
      </Card>
    </>
  );

  // 渲染Hoodie配置Tab
  const renderHoodieConfigTab = () => {
    const hoodieConfigData = parseHoodieConfig(record.hoodieConfig || '{}');
    
    return (
      <>
        <Card size="small">
          <Tabs defaultActiveKey="table" type="card">
            <TabPane tab="配置视图" key="table">
              <Table
                columns={hoodieConfigColumns}
                dataSource={hoodieConfigData}
                pagination={false}
                size="small"
                bordered
                scroll={{ x: 600, y: 400 }}
                locale={{
                  emptyText: '暂无配置信息',
                }}
              />
            </TabPane>
            
            <TabPane tab="JSON配置预览" key="json">
              <div style={{ 
                border: '1px solid #d9d9d9', 
                borderRadius: '6px',
                padding: '16px',
                minHeight: '400px',
                background: '#f6f6f6'
              }}>
                <pre style={{ 
                  margin: 0, 
                  fontFamily: 'monospace',
                  fontSize: '12px',
                  whiteSpace: 'pre-wrap',
                  wordBreak: 'break-word'
                }}>
                  {record.hoodieConfig ? 
                    JSON.stringify(JSON.parse(record.hoodieConfig), null, 2) : 
                    '// 暂无Hoodie配置'
                  }
                </pre>
              </div>
            </TabPane>
          </Tabs>
        </Card>
      </>
    );
  };

  // 渲染其他信息Tab
  const renderOtherInfoTab = () => (
    <>
      <Divider orientation="left">
        <Space>
          <TagsOutlined />
          标签信息
        </Space>
      </Divider>

      <Card size="small" style={{ marginBottom: 16 }}>
        <Descriptions column={1} size="small" bordered>
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

      <Divider orientation="left">
        <Space>
          <InfoCircleOutlined />
          统计信息
        </Space>
      </Divider>

      <Card size="small">
        <Descriptions bordered column={2} size="small">
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
          <Descriptions.Item label="创建时间">
            <Text>{dayjs(record.createdTime).format('YYYY-MM-DD HH:mm:ss')}</Text>
          </Descriptions.Item>
          <Descriptions.Item label="更新时间">
            <Text>{dayjs(record.updatedTime).format('YYYY-MM-DD HH:mm:ss')}</Text>
          </Descriptions.Item>
        </Descriptions>
      </Card>
    </>
  );

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
      width={1200}
      destroyOnClose
    >
      <Tabs 
        activeKey={activeTab} 
        onChange={setActiveTab}
        type="card"
        size="large"
      >
        <TabPane 
          tab={
            <Space>
              <InfoCircleOutlined />
              基本信息
            </Space>
          } 
          key="basic"
        >
          {renderBasicInfoTab()}
        </TabPane>
        
        <TabPane 
          tab={
            <Space>
              <TableOutlined />
              表结构
            </Space>
          } 
          key="schema"
        >
          {renderSchemaTab()}
        </TabPane>
        
        <TabPane 
          tab={
            <Space>
              <SettingOutlined />
              Hoodie配置
            </Space>
          } 
          key="hoodieConfig"
        >
          {renderHoodieConfigTab()}
        </TabPane>
        
        <TabPane 
          tab={
            <Space>
              <FileTextOutlined />
              其他信息
            </Space>
          } 
          key="other"
        >
          {renderOtherInfoTab()}
        </TabPane>
      </Tabs>
    </Modal>
  );
};

export default TableDetailModal; 