package com.atguigu.flink_exer.skew_data;

import com.atguigu.flink.datastreamapi.transform.ClickSource;
import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.pojo.UserViewCountPerWindow;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * keyBy之后开窗出现数据倾斜
 */
public class Test32_SkewDataWindow {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10011);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(2);
        env.disableOperatorChaining();

        env
                .addSource(new ClickSource())
                // 模拟倾斜数据
                .flatMap(new EtlStream())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner((event, l) -> event.ts))
                .keyBy(r -> r.username)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(3)))
                .aggregate(new AccFunction(), new ProcessFunction())
                .print();

        env.execute();
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
