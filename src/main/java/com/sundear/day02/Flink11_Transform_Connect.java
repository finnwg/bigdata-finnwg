package com.sundear.day02;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class Flink11_Transform_Connect {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        env.setParallelism(1);

        //读取数据创建流
        DataStreamSource<String> lineDS1 = env.socketTextStream("hadoop102", 9999);
        DataStreamSource<String> lineDS2 = env.socketTextStream("hadoop102", 9999);


        //将其中一个流转换数据类型
        SingleOutputStreamOperator<Integer> mapDS = lineDS1.map(String::length);

        //合并两个流
        ConnectedStreams<Integer, String> connectDS = mapDS.connect(lineDS2);

        //处理合并后的流
        SingleOutputStreamOperator<Object> coMapDS = connectDS.map(new CoMapFunction<Integer, String, Object>() {
            @Override
            public Object map1(Integer value) throws Exception {
                return value;
            }

            @Override
            public Object map2(String value) throws Exception {
                return value;
            }
        });

        //打印结果
        coMapDS.print();

        //启动任务
        env.execute();
    }
}
