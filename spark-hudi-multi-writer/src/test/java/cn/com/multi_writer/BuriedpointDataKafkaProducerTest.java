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
 * 埋点数据Kafka生产者测试类
 * 功能：读取buriedpoint_data.json文件并发送消息到Kafka topic
 * 
 * @author 系统生成
 * @version 1.0
 */
public class BuriedpointDataKafkaProducerTest {
    
    private static final Logger logger = LoggerFactory.getLogger(BuriedpointDataKafkaProducerTest.class);
    
    // Kafka配置常量
    private static final String KAFKA_BOOTSTRAP_SERVERS = "10.94.162.31:9092";
    private static final String KAFKA_TOPIC_NAME = "tracker_marketing";
    private static final String JSON_FILE_PATH = "/buriedpoint_data.json";
    
    /**
     * 测试发送埋点JSON数据到Kafka topic
     * 该测试用例会读取buriedpoint_data.json文件，解析JSON对象，并发送到Kafka
     */
    @Test
    public void testSendBuriedpointDataToKafka() {
        logger.info("开始测试发送埋点JSON数据到Kafka...");
        
        KafkaProducer<String, String> producer = null;
        try {
            // 创建Kafka生产者
            producer = createKafkaProducer();
            
            // 读取JSON文件内容
            String jsonContent = readJsonFile();
            
            // 验证JSON格式并发送
            JSONObject jsonObject = JSON.parseObject(jsonContent);
            if (jsonObject != null) {
                sendMessageToKafka(producer, jsonContent);
                logger.info("埋点JSON数据发送完成");
            } else {
                logger.error("无效的JSON数据格式");
                throw new RuntimeException("JSON数据格式错误");
            }
            
        } catch (Exception e) {
            logger.error("发送埋点JSON数据到Kafka时发生错误", e);
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
            
            String result = content.toString().trim();
            logger.info("成功读取JSON文件，文件大小: {} 字符", result.length());
            return result;
        }
    }
    
    /**
     * 发送消息到Kafka topic
     * @param producer Kafka生产者
     * @param jsonMessage JSON消息内容
     */
    private void sendMessageToKafka(KafkaProducer<String, String> producer, String jsonMessage) {
        try {
            // 解析JSON数据，从中提取deviceId作为消息key
            JSONObject jsonObject = JSON.parseObject(jsonMessage);
            String deviceId = jsonObject.getString("deviceId");
            
            // 创建生产者记录，使用deviceId作为key
            ProducerRecord<String, String> record = new ProducerRecord<>(
                KAFKA_TOPIC_NAME, 
                deviceId, // 使用deviceId作为key，确保相同设备的消息进入同一分区
                jsonMessage // Value为JSON字符串
            );
            
            // 发送消息并获取Future对象
            Future<RecordMetadata> future = producer.send(record);
            
            // 获取发送结果（同步等待）
            RecordMetadata metadata = future.get();
            
            logger.info("埋点消息发送成功 - Topic: {}, Partition: {}, Offset: {}, Key: {}, 消息大小: {} 字节", 
                metadata.topic(), 
                metadata.partition(), 
                metadata.offset(),
                deviceId,
                jsonMessage.getBytes(StandardCharsets.UTF_8).length
            );
            
        } catch (Exception e) {
            logger.error("发送埋点消息失败，消息内容（前100字符）: {}", 
                jsonMessage.substring(0, Math.min(100, jsonMessage.length())), 
                e);
            throw new RuntimeException("发送消息失败", e);
        }
    }
}
