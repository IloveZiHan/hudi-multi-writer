# TDSQL数据发送到Kafka测试说明

## 功能描述
该测试用例用于读取 `tdsql_data_1.json` 文件中的JSON对象，并将每个对象作为消息发送到指定的Kafka topic。

## 配置信息
- **Kafka服务器地址**: 10.94.162.31:9092
- **Kafka Topic名称**: rtdw_tdsql_alc  
- **消息格式**: Key为null，Value为JSON字符串
- **数据源文件**: /tdsql_data_1.json (位于resources目录下)

## 测试类信息
- **类名**: `cn.com.multi_writer.TdsqlDataKafkaProducerTest`
- **测试方法**: `testSendTdsqlDataToKafka()`
- **位置**: `src/test/java/cn/com/multi_writer/TdsqlDataKafkaProducerTest.java`

## 运行方式

### 1. 使用Maven命令行运行
```bash
cd hudi-multi-writer/spark-hudi-multi-writer
mvn test -Dtest=TdsqlDataKafkaProducerTest#testSendTdsqlDataToKafka
```

### 2. 使用IDE运行
1. 在IDE中打开项目
2. 找到测试类 `TdsqlDataKafkaProducerTest`
3. 右键点击 `testSendTdsqlDataToKafka` 方法
4. 选择 "Run Test" 或 "Debug Test"

## 运行前检查

### 1. 确保Kafka服务可用
确保Kafka服务器 `10.94.162.31:9092` 正常运行并可访问。

### 2. 确保Topic存在
确保Kafka中存在名为 `rtdw_tdsql_alc` 的topic，如果不存在可以使用以下命令创建：
```bash
kafka-topics.sh --create --topic rtdw_tdsql_alc --bootstrap-server 10.94.162.31:9092 --partitions 3 --replication-factor 1
```

### 3. 检查数据文件
确保 `src/main/resources/tdsql_data_1.json` 文件存在且包含有效的JSON数据。

## 执行流程
1. **读取文件**: 从resources目录读取 `tdsql_data_1.json` 文件
2. **解析JSON**: 将文件内容解析为独立的JSON对象
3. **创建生产者**: 使用配置创建Kafka生产者
4. **发送消息**: 逐个发送JSON对象到Kafka topic
5. **记录结果**: 记录每条消息的发送结果（partition、offset等）
6. **关闭资源**: 关闭Kafka生产者

## 预期结果
- 控制台会显示详细的日志信息
- 每条成功发送的消息都会记录Topic、Partition、Offset等信息
- 如果发送失败，会抛出异常并显示错误信息
- 测试完成后会显示总共发送的消息数量

## 故障排除

### 1. 连接异常
如果出现连接Kafka服务器的异常，请检查：
- 网络连接是否正常
- Kafka服务器地址和端口是否正确
- 防火墙设置是否允许连接

### 2. 文件读取异常  
如果出现文件读取异常，请检查：
- 文件路径是否正确
- 文件是否存在于resources目录中
- 文件编码是否为UTF-8

### 3. JSON解析异常
如果出现JSON解析异常，请检查：
- JSON文件格式是否正确
- 是否包含非法字符
- JSON对象是否完整

## 依赖项
测试使用以下主要依赖：
- `kafka-clients`: Kafka客户端库
- `fastjson2`: JSON解析库  
- `junit`: 单元测试框架
- `slf4j-api`: 日志接口

所有依赖已在项目的pom.xml中配置，无需额外安装。 