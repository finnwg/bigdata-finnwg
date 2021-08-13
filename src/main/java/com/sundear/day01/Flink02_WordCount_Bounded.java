package com.sundear.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink02_WordCount_Bounded {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取文件
        DataStreamSource<String> lineDS = env.readTextFile("input\\WordCount.txt");

        //转换数据格式
        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2DS = lineDS.flatMap(new MyFlatMap());

        KeyedStream<Tuple2<String, Integer>, Tuple> keyByDS = tuple2DS.keyBy(0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> sumDS = keyByDS.sum(1);


        //打印结果
        sumDS.print();

        //启动任务
        env.execute();
    }

    public static class MyFlatMap implements FlatMapFunction<String, Tuple2<String,Integer>>{
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] strings = value.split(" ");
            for (String string : strings) {
                out.collect(Tuple2.of(string,1));
            }
        }
    }

}
