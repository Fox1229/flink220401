package com.atguigu.flink.waterline;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

public class ProcessTimeSessionWaterMarkTest1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(
                        new SourceFunction<String>() {
                            @Override
                            public void run(SourceContext<String> ctx) throws Exception {
                                ctx.collectWithTimestamp("hello", 1000L);
                                ctx.collectWithTimestamp("hello", 4000L);
                                ctx.collectWithTimestamp("hello", 10000L);
                                ctx.emitWatermark(new Watermark(20000L));
                                ctx.collectWithTimestamp("hello", 7000L);
                            }

                            @Override
                            public void cancel() {

                            }
                        })
                .keyBy(r -> 1)
                .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
                .process(
                        new ProcessWindowFunction<String, String, Integer, TimeWindow>() {
                            @Override
                            public void process(Integer integer, ProcessWindowFunction<String, String, Integer, TimeWindow>.Context context, Iterable<String> elements, Collector<String> out) throws Exception {

                                out.collect("窗口" + context.window().getStart() + "~"
                                        + context.window().getEnd() + ", 数据个数：" + elements.spliterator().getExactSizeIfKnown());
                            }
                        })
                .print();

        env.execute();
    }
}
