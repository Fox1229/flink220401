package com.atguigu.flink.waterline;

import com.atguigu.flink.pojo.ProductViewCountPerWindow;
import com.atguigu.flink.pojo.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 每个用户最近一个小时的访问次数
 */
public class ProductClickCountTest1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .readTextFile("D:\\developer\\idea workspace\\flink220401\\data\\UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return new UserBehavior(fields[0], fields[1], fields[2], fields[3], Long.parseLong(fields[4]) * 1000L);
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                                    @Override
                                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                        return element.ts;
                                    }
                                })
                )
                .keyBy(r -> r.productId)
                .window(TumblingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new ProductAcc(), new ProductFun())
                .print();

        env.execute();
    }

    public static class ProductAcc implements AggregateFunction<UserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    public static class ProductFun extends ProcessWindowFunction<Long, ProductViewCountPerWindow, String, TimeWindow> {
        @Override
        public void process(String s, ProcessWindowFunction<Long, ProductViewCountPerWindow, String, TimeWindow>.Context context, Iterable<Long> elements, Collector<ProductViewCountPerWindow> out) throws Exception {
            out.collect(
                    new ProductViewCountPerWindow(s, elements.iterator().next(), context.window().getStart(), context.window().getEnd())
            );
        }
    }
}
