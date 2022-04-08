package com.atguigu.flink.waterline;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class ProcessTimeSessionWaterMarkTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(
                        new SourceFunction<String>() {
                            @Override
                            public void run(SourceContext<String> ctx) throws Exception {
                                ctx.collect("hello");
                                Thread.sleep(2000L);
                                ctx.collect("hello");
                                Thread.sleep(10 * 1000L);

                                // 这两条数据发送过去之后，由于程序执行结束，导致没有闭合，process并没有触发，不会打印
                                // process方法的触发条件：窗口闭合
                                ctx.collect("hello");
                                ctx.collect("hello");
                            }

                            @Override
                            public void cancel() {

                            }
                        }
                )
                .keyBy(r -> 1)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                .process(
                        new ProcessWindowFunction<String, String, Integer, TimeWindow>() {
                            @Override
                            public void process(Integer integer, ProcessWindowFunction<String, String, Integer, TimeWindow>.Context context, Iterable<String> elements, Collector<String> out) throws Exception {

                                out.collect("窗口" + new Timestamp(context.window().getStart()) + "~" +
                                        "" + new Timestamp(context.window().getEnd()) + "里面一共有" + elements.spliterator().getExactSizeIfKnown() + "" +
                                        "条数据");
                            }
                        })
                .print();


        env.execute();
    }
}
