package com.sundear.day02;

import com.sundear.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class Flink02_Source_Collection {
    public static void main(String[] args) {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        env.setParallelism(1);

        //从集合中读取数据
        DataStreamSource<WaterSensor> waterSensorDS = env.fromCollection(
                Arrays.asList(
                        new WaterSensor("ws_001", 1577844001L, 45),
                        new WaterSensor("ws_002", 1577844015L, 43),
                        new WaterSensor("ws_003", 1577844020L, 42)
                )
        );


        //打印结果
        waterSensorDS.print();

        //启动任务
        try {
            env.execute();
        } catch (Exception e) {
            System.err.println("任务启动失败！！!");
            e.printStackTrace();
        }
    }
}
