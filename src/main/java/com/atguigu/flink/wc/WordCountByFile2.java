package com.atguigu.flink.wc;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 从文件获取数据实现WordCount
 */
public class WordCountByFile2 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .readTextFile("D:\\developer\\idea workspace\\flink220401\\data\\word.txt")
                .flatMap((String in, Collector<Tuple2<String, Integer>> out) -> {
                    String[] fields = in.split(" ");
                    for (String word : fields) {
                        out.collect(Tuple2.of(word, 1));
                    }
                }).returns(new TypeHint<Tuple2<String, Integer>>() {
                }).keyBy(t -> t.f0)
                .sum("f1")
                .print();

        // 执行环境
        env.execute();
    }
}