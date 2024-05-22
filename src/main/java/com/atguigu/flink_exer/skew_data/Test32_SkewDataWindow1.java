package com.atguigu.flink_exer.skew_data;

import com.atguigu.flink.datastreamapi.transform.ClickSource;
import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.pojo.UserViewCountPerWindow;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * 解决keyBy后开窗出现的数据倾斜
 */
public class Test32_SkewDataWindow1 {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10012);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(2);
        env.disableOperatorChaining();

        env
                .addSource(new ClickSource())
                // 模拟倾斜数据
                .flatMap(new EtlStream())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner((event, l) -> event.ts))
                // 将username拼接随机数打散 keyBy 开窗 聚合
                .map(new MapAddRandomFun())
                .keyBy(event -> event.username)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(3)))
                .aggregate(new AccFunction(), new ProcessFunction())
                // 去除随机数，按照原username + 窗口结束时间keyBy 聚合
                .map(new MapDelRandomFun())
                .keyBy(bean -> bean.username + bean.windowStopTime)
                .reduce(new MyReduceFun())
                .print();

        env.execute();
    }

    public static class MyReduceFun implements ReduceFunction<UserViewCountPerWindow> {
        @Override
        public UserViewCountPerWindow reduce(UserViewCountPerWindow value1, UserViewCountPerWindow value2) throws Exception {
            value1.count = value1.count + value2.count;
            return value1;
        }
    }

    public static class ProcessFunction extends ProcessWindowFunction<Long, UserViewCountPerWindow, String, TimeWindow> {
        @Override
        public void process(String username, ProcessWindowFunction<Long, UserViewCountPerWindow, String, TimeWindow>.Context context, Iterable<Long> elements, Collector<UserViewCountPerWindow> out) throws Exception {
            out.collect(
                    new UserViewCountPerWindow(
                            username,
                            elements.iterator().next(),
                            context.window().getStart(),
                            context.window().getEnd()
                    )
            );
        }
    }

    public static class AccFunction implements AggregateFunction<Event, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event value, Long accumulator) {
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

    public static class MapDelRandomFun implements MapFunction<UserViewCountPerWindow, UserViewCountPerWindow> {
        @Override
        public UserViewCountPerWindow map(UserViewCountPerWindow uv) throws Exception {
            String[] array = uv.username.split("-");
            uv.username = array[0];
            return uv;
        }
    }

    public static class MapAddRandomFun implements MapFunction<Event, Event> {
        private Random random = new Random();

        @Override
        public Event map(Event event) throws Exception {
            int num = random.nextInt(6);
            event.username = event.username + "-" + num;
            return event;
        }
    }

    public static class EtlStream implements FlatMapFunction<Event, Event> {
        private Random random = new Random();

        @Override
        public void flatMap(Event event, Collector<Event> out) throws Exception {
            if ("Mary".equals(event.username)) {
                int num = (random.nextInt(10) + 1) * 1000;
                for (int i = 0; i < num; i++) {
                    out.collect(event);
                }
            } else {
                out.collect(event);
            }
        }
    }
}
