package com.atguigu.flink.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 从文件获取数据实现WordCount
 */
public class WordCountByFile {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .readTextFile("D:\\developer\\idea workspace\\flink220401\\data\\word.txt")
                .flatMap(new FlatMapData())
                .keyBy(new KeyByData())
                .reduce(new SumData())
                .print();

        // 执行环境
        env.execute();
    }

    public static class FlatMapData implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String in, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] fields = in.split(" ");
            for (String field : fields) {
                out.collect(Tuple2.of(field, 1));
            }
        }
    }

    public static class KeyByData implements KeySelector<Tuple2<String, Integer>, String> {
        @Override
        public String getKey(Tuple2<String, Integer> in) throws Exception {
            return in.f0;
        }
    }

    // ReduceFunction只有一个泛型
    // 因为reduce的输入、输出和累加器的泛型是一样的
    public static class SumData implements ReduceFunction<Tuple2<String, Integer>> {
        // 定义输入数据和累加器的累加规则
        // 两个参数：一个是输入事件，一个是累加器
        @Override
        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
            return Tuple2.of(t1.f0, t1.f1 + t2.f1);
        }
    }
}
