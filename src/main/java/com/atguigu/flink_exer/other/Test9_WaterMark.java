package com.atguigu.flink_exer.other;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.time.Duration;

public class Test9_WaterMark {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ÿ��1���Ӳ���һ��ˮλ��
        env.getConfig().setAutoWatermarkInterval(60 * 1000);

        env
                .socketTextStream("hadoop102", 6666)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        if (value != null && value.length() > 2) {
                            String[] arr = value.split(" ");
                            out.collect(
                                    Tuple2.of(arr[0], Long.parseLong(arr[1]) * 1000)
                            );
                        }
                    }
                })
                // ����ˮλ���ӳ�ʱ�估ˮλ���ֶ�
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner((t, l) -> t.f1))
                .keyBy(r -> r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5L)))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>.Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        long cnt = elements.spliterator().getExactSizeIfKnown();
                        out.collect(
                                s + "�ڴ���" + context.window().getStart() + "~" + context.window().getEnd() +
                                        "���ʴ���" + cnt
                        );
                    }
                })
                .print();

        env.execute();
    }
}
