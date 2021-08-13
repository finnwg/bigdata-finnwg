package com.sundear.day02;

import com.sundear.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink04_Source_Socket {
    public static void main(String[] args) {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        env.setParallelism(1);

        //读取端口数据
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 9999);

        //转换为javabean
        SingleOutputStreamOperator<WaterSensor> mapDS = socketDS.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] strings = value.split(",");
                return new WaterSensor(
                        strings[0],
                        Long.parseLong(strings[1]),
                        Integer.parseInt(strings[2])
                );
            }
        });

        //打印结果
        mapDS.print();

        //启动任务
        try {
            env.execute();
        } catch (Exception e) {
            System.err.println("启动任务失败！！！");
            e.printStackTrace();
        }
    }
}
