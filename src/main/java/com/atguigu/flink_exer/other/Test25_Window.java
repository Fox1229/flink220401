package com.atguigu.flink_exer.other;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import java.sql.Timestamp;
import java.time.Duration;

public class Test25_Window {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 定义侧输出流标签
        OutputTag<Tuple2<String, Long>> laterData = new OutputTag<Tuple2<String, Long>>("laterData") {};

        SingleOutputStreamOperator<String> resStream = env
                .socketTextStream("hadoop102", 6666)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        String[] array = value.split(" ");
                        out.collect(Tuple2.of(array[0], Long.parseLong(array[1]) * 1000));
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner((t, l) -> t.f1))
                .keyBy(r -> r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5L)))
                .allowedLateness(Time.seconds(3L))
                .sideOutputLateData(laterData)
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>.Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        out.collect(
                                "窗口" + new Timestamp(context.window().getStart()) + "~" + new Timestamp(context.window().getEnd()) +
                                        "有" + elements.spliterator().getExactSizeIfKnown() + "条数据"
                        );
                    }
                });

        resStream.print("主流");
        resStream.getSideOutput(laterData).print("侧输出流");

        env.execute();
    }
}
