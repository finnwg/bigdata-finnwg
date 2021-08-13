package com.sundear.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class Flink01_WordCount__Batch {
    public static void main(String[] args) throws Exception {
        //获取环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //读取文件
        DataSource<String> lineDS = env.readTextFile("input/WordCount.txt");

        //转换数据格式flatMap
        FlatMapOperator<String, String> wordDS = lineDS.flatMap(new MyFlatMap());
        FlatMapOperator<String, String> flatMap = lineDS.flatMap(new MyFlatMap());

        //转换为元组
        MapOperator<String, Tuple2<String, Integer>> word2OneDS = wordDS.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<>(s, 1);
                //return Tuple2.of(s,1); //也可
            }
        });

        MapOperator<String, Tuple2<String, Integer>> map = flatMap.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        });

        /*FlatMapOperator<String, Tuple2<String, Long>> wordAndOne = lineDS
                .flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] split = line.split(" ");
                    for (String word : split) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG));
                //当Lambda表达式使用 java 泛型的时候, 由于泛型擦除的存在, 需要显示的声明类型信息
*/
        //分组
        //UnsortedGrouping<Tuple2<String, Integer>> groupBy = word2OneDS.groupBy(0);
        //UnsortedGrouping<Tuple2<String, Long>> tuple2UnsortedGrouping = wordAndOne.groupBy(0);
        UnsortedGrouping<Tuple2<String, Integer>> tuple2UnsortedGrouping = map.groupBy(0);


        //聚合
        //AggregateOperator<Tuple2<String, Integer>> result = groupBy.sum(1);
        //AggregateOperator<Tuple2<String, Long>> sum = tuple2UnsortedGrouping.sum(1);
        AggregateOperator<Tuple2<String, Integer>> sumDS = tuple2UnsortedGrouping.sum(1);

        //打印
        //result.print();
        //sum.print();
        sumDS.print();


    }

    public static class MyFlatMap implements FlatMapFunction<String,String>{
        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            String[] strings = value.split(" ");
            for (String string : strings) {
                out.collect(string);
            }
        }
    }

    /*public static class MyFlatMap implements FlatMapFunction<String ,String>{

        @Override
        public void flatMap(String s, Collector<String> collector) throws Exception {
            String[] words = s.split(" ");

            for (String word : words) {
                collector.collect(word);
            }

        }
    }*/
}
