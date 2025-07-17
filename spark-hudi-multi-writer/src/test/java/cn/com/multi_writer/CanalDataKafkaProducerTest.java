package cn.com.multi_writer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Canal数据Kafka生产者测试类
 * 功能：读取canal_data.json文件并发送消息到Kafka topic
 * 
 * @author 系统生成
 * @version 1.0
 */
public class CanalDataKafkaProducerTest {
    
    private static final Logger logger = LoggerFactory.getLogger(CanalDataKafkaProducerTest.class);
    
    // Kafka配置常量
    private static final String KAFKA_BOOTSTRAP_SERVERS = "10.94.162.31:9092";
    private static final String KAFKA_TOPIC_NAME = "rtdw_mysql_sms";
    private static final String JSON_FILE_PATH = "/canal_data.json";
    
    /**
     * 测试发送Canal JSON数据到Kafka topic
     * 该测试用例会读取canal_data.json文件，解析其中的JSON对象，并逐个发送到Kafka
     */
    @Test
    public void testSendCanalDataToKafka() {
        logger.info("开始测试发送Canal JSON数据到Kafka...");
        
        KafkaProducer<String, String> producer = null;
        try {
            // 创建Kafka生产者
            producer = createKafkaProducer();
            
            // 读取JSON文件内容
            String jsonContent = readJsonFile();
            
            // 解析JSON文件，提取每个JSON对象
            String[] jsonObjects = parseJsonObjects(jsonContent);
            
            // 逐个发送JSON对象到Kafka
            for (int i = 0; i < jsonObjects.length; i++) {
                String jsonObject = jsonObjects[i];
                if (jsonObject != null && !jsonObject.trim().isEmpty()) {
                    sendMessageToKafka(producer, jsonObject, i + 1);
                    
                    // 添加短暂延迟，避免发送过快
                    Thread.sleep(100);
                }
            }
            
            logger.info("所有Canal JSON数据发送完成，共发送 {} 条消息", jsonObjects.length);
            
        } catch (Exception e) {
            logger.error("发送Canal JSON数据到Kafka时发生错误", e);
            throw new RuntimeException("测试失败", e);
        } finally {
            // 关闭生产者
            if (producer != null) {
                producer.close();
                logger.info("Kafka生产者已关闭");
            }
        }
    }
    
    /**
     * 创建Kafka生产者
     * @return KafkaProducer实例
     */
    private KafkaProducer<String, String> createKafkaProducer() {
        Properties props = new Properties();
        
        // 配置Kafka服务器地址
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        
        // 配置序列化器
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // 配置生产者参数 - 保证消息可靠性
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // 等待所有副本确认
        props.put(ProducerConfig.RETRIES_CONFIG, 3); // 重试次数
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384); // 批次大小
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1); // 延迟时间（毫秒）
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432); // 缓冲区大小 32MB
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 1048576); // 最大请求大小 1MB
        
        logger.info("创建Kafka生产者，服务器地址: {}, Topic: {}", KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_NAME);
        return new KafkaProducer<>(props);
    }
    
    /**
     * 读取JSON文件内容
     * @return 文件内容字符串
     * @throws IOException 文件读取异常
     */
    private String readJsonFile() throws IOException {
        logger.info("开始读取JSON文件: {}", JSON_FILE_PATH);
        
        try (InputStream inputStream = getClass().getResourceAsStream(JSON_FILE_PATH);
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            
            if (inputStream == null) {
                throw new IOException("找不到文件: " + JSON_FILE_PATH);
            }
            
            StringBuilder content = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line).append("\n");
            }
            
            String result = content.toString();
            logger.info("成功读取JSON文件，文件大小: {} 字符", result.length());
            return result;
        }
    }
    
    /**
     * 解析JSON文件内容，提取每个独立的JSON对象
     * Canal数据文件包含多个独立的JSON对象，每个对象占多行
     * @param jsonContent 完整的JSON文件内容
     * @return JSON对象数组
     */
    private String[] parseJsonObjects(String jsonContent) {
        logger.info("开始解析Canal JSON文件内容...");
        
        // 将文件内容按行分割，然后重新组合每个JSON对象
        String[] lines = jsonContent.split("\n");
        StringBuilder currentJsonObject = new StringBuilder();
        java.util.List<String> jsonObjects = new java.util.ArrayList<>();
        int braceCount = 0;
        boolean inJsonObject = false;
        
        for (String line : lines) {
            line = line.trim();
            if (line.isEmpty()) {
                continue;
            }
            
            // 计算大括号数量来判断JSON对象边界
            for (char c : line.toCharArray()) {
                if (c == '{') {
                    braceCount++;
                    if (!inJsonObject) {
                        inJsonObject = true;
                    }
                } else if (c == '}') {
                    braceCount--;
                }
            }
            
            if (inJsonObject) {
                currentJsonObject.append(line);
                
                // 当大括号平衡时，表示一个完整的JSON对象
                if (braceCount == 0) {
                    String jsonStr = currentJsonObject.toString();
                    // 使用FastJSON验证JSON格式
                    try {
                        JSONObject jsonObject = JSON.parseObject(jsonStr);
                        if (jsonObject != null) {
                            jsonObjects.add(jsonStr);
                            logger.debug("解析到Canal JSON对象 {}，长度: {} 字符", jsonObjects.size(), jsonStr.length());
                        }
                    } catch (Exception e) {
                        logger.warn("无效的JSON对象: {}, 错误: {}", 
                            jsonStr.substring(0, Math.min(100, jsonStr.length())), e.getMessage());
                    }
                    currentJsonObject = new StringBuilder();
                    inJsonObject = false;
                }
            }
        }
        
        logger.info("Canal JSON解析完成，共解析到 {} 个JSON对象", jsonObjects.size());
        return jsonObjects.toArray(new String[0]);
    }
    
    /**
     * 发送消息到Kafka topic
     * @param producer Kafka生产者
     * @param jsonMessage JSON消息内容
     * @param messageIndex 消息序号
     */
    private void sendMessageToKafka(KafkaProducer<String, String> producer, String jsonMessage, int messageIndex) {
        try {
            // 创建生产者记录，Key为null，Value为JSON字符串
            ProducerRecord<String, String> record = new ProducerRecord<>(
                KAFKA_TOPIC_NAME, 
                null, // Key设置为null，按照要求
                jsonMessage // Value为JSON字符串
            );
            
            // 发送消息并获取Future对象
            Future<RecordMetadata> future = producer.send(record);
            
            // 获取发送结果（同步等待）
            RecordMetadata metadata = future.get();
            
            logger.info("Canal消息 {} 发送成功 - Topic: {}, Partition: {}, Offset: {}, 消息大小: {} 字节", 
                messageIndex, 
                metadata.topic(), 
                metadata.partition(), 
                metadata.offset(),
                jsonMessage.getBytes(StandardCharsets.UTF_8).length
            );
            
        } catch (Exception e) {
            logger.error("发送Canal消息 {} 失败，消息内容（前100字符）: {}", 
                messageIndex, 
                jsonMessage.substring(0, Math.min(100, jsonMessage.length())), 
                e);
            throw new RuntimeException("发送消息失败", e);
        }
    }
}
