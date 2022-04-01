package com.atguigu.flink.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountBySocket1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("hadoop102", 9999)
                .flatMap(new FlatMapData())
                .keyBy(t -> t.f0)
                .sum("f1")
                .print();

        env.execute();
    }

    public static class FlatMapData implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String in, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] fields = in.split(" ");
            for (String word : fields) {
                out.collect(Tuple2.of(word, 1));
            }
        }
    }
}
