package com.atguigu.flink1.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountBySocket {

    public static void main(String[] args) throws Exception {

        // 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);

        DataStreamSource<String> socketStream = env.socketTextStream("hadoop102", 9999);
        socketStream
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                             @Override
                             public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                                 String[] wordArr = value.split(" ");
                                 for (String word : wordArr) {
                                     out.collect(Tuple2.of(word, 1));
                                 }
                             }
                         }
                )
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                           @Override
                           public String getKey(Tuple2<String, Integer> inputValue) throws Exception {
                               return inputValue.f0;
                           }
                       }
                )
                .sum("f1")
                .print();

        // 执行
        env.execute();
    }
}
