package com.atguigu.flink_exer.other;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Test10_WaterMark {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, String>() {
                    @Override
                    public void processElement(Tuple2<String, Long> value, KeyedProcessFunction<String, Tuple2<String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect("����" + value + "�����ǰˮλ��Ϊ" + ctx.timerService().currentWatermark());
                        // ע�ᶨʱ��
                        ctx.timerService().registerEventTimeTimer(value.f1 + 1000L);
                        out.collect("ע��" + (value.f1 + 1000L) + "�Ķ�ʱ��");
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, Tuple2<String, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("��ʱ����������ǰˮλ��Ϊ" + ctx.timerService().currentWatermark());
                    }
                })
                .print();

        env.execute();
    }
}
