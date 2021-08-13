package com.sundear.day02;

import com.sundear.bean.WaterSensor;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.omg.PortableInterceptor.INACTIVE;

public class Flink09_Transform_RichFlatMap {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        env.setParallelism(1);

        //读取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //转换为javabean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.flatMap(new MyFlatMap());

        //打印输出
        waterSensorDS.print();

        //启动任务
        env.execute();
    }
    public static class MyFlatMap extends RichFlatMapFunction<String, WaterSensor> {
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open方法调用");
        }

        @Override
        public void flatMap(String value, Collector<WaterSensor> out) throws Exception {
            String[] split = value.split(",");
            out.collect(new WaterSensor(
                    split[0],
                    Long.valueOf(split[1]),
                    Integer.parseInt(split[2])
            ));
        }

        @Override
        public void close() throws Exception {
            System.out.println("slose方法调用");
        }
    }
}
