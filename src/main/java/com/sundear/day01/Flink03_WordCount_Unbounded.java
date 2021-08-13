package com.sundear.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink03_WordCount_Unbounded  {
    public static void main(String[] args) {

        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //为整个程序设置并行度
        //env.setParallelism(1);

        //全局禁用任务连
        //env.disableOperatorChaining();


        //parameter工具从程序启动参数中提取配置项
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hostname = parameterTool.get("hostname");
        int port = parameterTool.getInt("port");

        //读取数据流
        DataStreamSource<String> socketDS = env.socketTextStream(hostname, port);

        //算子单独设置并行度
        //socketDS.setParallelism(1);
        //算子设置共享组
        //socketDS.slotSharingGroup("red");


        //转换结构
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMapDS = socketDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] strings = value.split(" ");
                for (String string : strings) {
                    out.collect(Tuple2.of(string, 1));
                }
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT));
        //算子单独禁用任务连
        //flatMapDS.disableChaining();

        //分组
        //KeyedStream<Tuple2<String, Integer>, Tuple> keyByDS = flatMapDS.keyBy(0);
        KeyedStream<Tuple2<String, Integer>, String> keyByDS = flatMapDS.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        //聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumDS = keyByDS.sum(1);
        //开启新的任务连
        //sumDS.startNewChain();

        //打印结果
        sumDS.print();

        //启动任务
        try {
            env.execute();
        } catch (Exception e) {
            System.err.println("任务启动失败！！！");
            e.printStackTrace();
        }
    }
}
