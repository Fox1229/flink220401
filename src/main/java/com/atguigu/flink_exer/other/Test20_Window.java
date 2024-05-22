package com.atguigu.flink_exer.other;

import com.atguigu.flink.datastreamapi.transform.ClickSource;
import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.pojo.UserViewCountPerWindow;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 每个用户在每个5秒钟滚动窗口中的浏览次数
 */
public class Test20_Window {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner((event, l) -> event.ts))
                .keyBy(event -> event.username)
                .window(TumblingEventTimeWindows.of(Time.seconds(5L)))
                .process(new ProcessFunction())
                .print();

        env.execute();
    }

    /**
     * 在只使用ProcessWindowFunction的情况下，process方法迭代器包含属于窗口的所有数据，会对内存造成压力
     */
    public static class ProcessFunction extends ProcessWindowFunction<Event, UserViewCountPerWindow, String, TimeWindow> {
        @Override
        public void process(String username, ProcessWindowFunction<Event, UserViewCountPerWindow, String, TimeWindow>.Context context, Iterable<Event> elements, Collector<UserViewCountPerWindow> out) throws Exception {
            long count = 0L;
            for (Event event : elements) {
                count += 1;
            }

            out.collect(
                    new UserViewCountPerWindow(
                            username,
                            count,
                            context.window().getStart(),
                            context.window().getEnd()
                    )
            );
        }
    }
}
