package com.sundear.day02;

import com.sundear.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink07_Transform_Map {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        env.setParallelism(1);

        //读取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //转换为javabean
        SingleOutputStreamOperator<WaterSensor> mapDS = socketTextStream.map(new MyMap());

        //打印输出
        mapDS.print();

        //启动任务
        env.execute();
    }


    public static class MyMap implements MapFunction<String,WaterSensor> {

        @Override
        public WaterSensor map(String value) throws Exception {
            String[] split = value.split(",");
            return new WaterSensor(
                    split[0],
                    Long.valueOf(split[1]),
                    Integer.parseInt(split[2])
            );
        }
    }
}
