package com.sundear.day02;

import com.sundear.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class Flink03_Source_File {
    public static void main(String[] args) {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从文件中读取数据
        DataStreamSource<String> lineDS = env.readTextFile("input/watersensor.txt");

        //转换为javabean
        SingleOutputStreamOperator<WaterSensor> mapDS = lineDS.map(new MyMap());

        //打印数据
        mapDS.print();

        //启动任务
        try {
            env.execute();
        } catch (Exception e) {
            System.err.println("任务启动失败！！！");
            e.printStackTrace();
        }

    }

    public static class MyMap implements MapFunction<String,WaterSensor>{
        @Override
        public WaterSensor map(String value) throws Exception {
            String[] strings = value.split(",");
            return new WaterSensor(
                    strings[0],
                    Long.parseLong(strings[1]),
                    Integer.parseInt(strings[2])
            );
        }
    }
}
