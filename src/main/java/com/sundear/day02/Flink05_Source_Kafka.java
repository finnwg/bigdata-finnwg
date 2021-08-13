package com.sundear.day02;

import com.sundear.bean.WaterSensor;
import com.sundear.utils.ApplicationUtils;
import com.sundear.utils.KafkaUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class Flink05_Source_Kafka {
    public static void main(String[] args) {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        env.setParallelism(1);



        //读取kafka数据
        DataStreamSource<String> kafkaDS = env.addSource(KafkaUtils.getKafkaSource());

        SingleOutputStreamOperator<String> map = kafkaDS.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        });


        //将数据转换为javabean
        /*SingleOutputStreamOperator<WaterSensor> mapDS = kafkaDS.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] strings = value.split(",");
                return new WaterSensor(
                        strings[0],
                        Long.valueOf(strings[1]),
                        Integer.parseInt(strings[2])
                );
            }
        });*/

        //打印数据
        map.print();

        //启动任务
        try {
            env.execute();
        } catch (Exception e) {
            System.err.println("启动任务失败！！！");
            e.printStackTrace();
        }
    }
}
