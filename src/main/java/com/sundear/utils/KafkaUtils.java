package com.sundear.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class KafkaUtils {

    //kafka属性配置
    private static String bootstrapServers = ApplicationUtils.getValue("kafka.bootstrapServers");
    private static String autoOffsetReset = ApplicationUtils.getValue("kafka.autoOffsetReset");
    private static String topic = ApplicationUtils.getValue("kafka.topic");
    private static String groupId = ApplicationUtils.getValue("kafka.groupId");

    //配置加载项
    private static Properties properties = new Properties();

    //静态内部类加载配置项
    static{
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,autoOffsetReset);
        //properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        //properties.setProperty(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG,topic);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
    }


    //返回kafka消费者数据源
    public static FlinkKafkaConsumer<String> getKafkaSource() {
        return new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),properties);
    }

}
