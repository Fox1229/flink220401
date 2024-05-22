package com.atguigu.flink_exer.other;

import com.atguigu.flink.pojo.UserBehavior;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;

/**
 * 使用布隆过滤器统计每小时UV数
 */
public class Test7_UVByBloomFilter {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .readTextFile("D:\\developer\\idea workspace\\flink220401\\data\\UserBehavior.csv")
                .flatMap(new EtlFunction())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                        .withTimestampAssigner((bean, l) -> bean.ts))
                .keyBy(r -> 1)
                .window(TumblingEventTimeWindows.of(Time.hours(1L)))
                .aggregate(new MyBloom(), new MyFun())
                .print();

        env.execute();
    }

    public static class MyFun extends ProcessWindowFunction<Long, String, Integer, TimeWindow> {
        @Override
        public void process(Integer integer, ProcessWindowFunction<Long, String, Integer, TimeWindow>.Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            out.collect(
                    "窗口 " + new Timestamp(context.window().getStart()) + "~" + new Timestamp(context.window().getEnd()) +
                            " UV数量为 " + elements.iterator().next()
            );
        }
    }

    public static class MyBloom implements AggregateFunction<UserBehavior, Tuple2<BloomFilter<String>, Long>, Long> {
        @Override
        public Tuple2<BloomFilter<String>, Long> createAccumulator() {
            return Tuple2.of(
                    BloomFilter.create(
                            // 带去重的类型
                            Funnels.stringFunnel(StandardCharsets.UTF_8),
                            // 待去重的数据规模
                            1000000,
                            // 误判率
                            0.01
                    ),
                    // 统计值：如果用户之前一定没来过，统计值 + 1
                    0L
            );
        }

        @Override
        public Tuple2<BloomFilter<String>, Long> add(UserBehavior value, Tuple2<BloomFilter<String>, Long> accumulator) {
            // 如果用户之前一定没来过，统计值 + 1
            if (!accumulator.f0.mightContain(value.userId)) {
                // 将对应位置为1
                accumulator.f0.put(value.userId);
                // 统计值 + 1
                accumulator.f1 += 1;
            }

            return accumulator;
        }

        @Override
        public Long getResult(Tuple2<BloomFilter<String>, Long> accumulator) {
            return accumulator.f1;
        }

        @Override
        public Tuple2<BloomFilter<String>, Long> merge(Tuple2<BloomFilter<String>, Long> a, Tuple2<BloomFilter<String>, Long> b) {
            return null;
        }
    }

    public static class EtlFunction implements FlatMapFunction<String, UserBehavior> {
        @Override
        public void flatMap(String value, Collector<UserBehavior> out) throws Exception {
            String[] array = value.split(",");
            if ("pv".equals(array[3])) {
                out.collect(
                        new UserBehavior(
                                array[0],
                                array[1],
                                array[2],
                                array[3],
                                Long.parseLong(array[4]) * 1000
                        )
                );
            }
        }
    }
}
