package com.sundear.day02;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;



public class Flink10_Transform_RichFilter {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        env.setParallelism(1);

        //读取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //筛选水位大于30的数据
        SingleOutputStreamOperator<String> filterDS = socketTextStream.filter(new MyFilter());

        //打印结果
        filterDS.print();

        //启动任务
        env.execute();
    }
    public static class MyFilter extends RichFilterFunction<String> {
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open方法被调用");
        }

        @Override
        public boolean filter(String value) throws Exception {
            String[] split = value.split(",");
            return Integer.parseInt(split[2]) > 30;
        }

        @Override
        public void close() throws Exception {
            System.out.println("close方法被调用");
        }
    }
}

