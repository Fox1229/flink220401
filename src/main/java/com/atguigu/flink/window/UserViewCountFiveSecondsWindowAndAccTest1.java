package com.atguigu.flink.window;

import com.atguigu.flink.datastreamapi.transform.ClickSource;
import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.pojo.UserViewCountPerWindow;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class UserViewCountFiveSecondsWindowAndAccTest1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .keyBy(r -> r.username)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new CountAgg(), new WindowResult())
                .print();

        env.execute();
    }

    public static class CountAgg implements AggregateFunction<Event, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event value, Long accumulator) {
            return accumulator + 1L;
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

    public static class WindowResult extends ProcessWindowFunction<Long, UserViewCountPerWindow, String, TimeWindow> {
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
}
