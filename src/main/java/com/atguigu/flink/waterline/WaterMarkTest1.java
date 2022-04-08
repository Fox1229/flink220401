package com.atguigu.flink.waterline;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class WaterMarkTest1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .socketTextStream("hadoop102", 9999)
                .map(
                        new MapFunction<String, Tuple2<String, Long>>() {
                            @Override
                            public Tuple2<String, Long> map(String value) throws Exception {
                                String[] fields = value.split(" ");
                                return Tuple2.of(fields[0], Long.parseLong(fields[1]) * 1000);
                            }
                        })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                            @Override
                                            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                                return element.f1;
                                            }
                                        })
                )
                .keyBy(r -> r.f0)
                .process(
                        new KeyedProcessFunction<String, Tuple2<String, Long>, String>() {
                            @Override
                            public void processElement(Tuple2<String, Long> value, KeyedProcessFunction<String, Tuple2<String, Long>, String>.Context ctx, Collector<String> out) throws Exception {

                                out.collect("数据" + value + "到达，当前水位线：" + ctx.timerService().currentWatermark());
                                ctx.timerService().registerEventTimeTimer(value.f1 + 9999);
                                out.collect("注册了" + (value.f1 + 9999) + "的定时器");
                            }

                            @Override
                            public void onTimer(long timestamp, KeyedProcessFunction<String, Tuple2<String, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                                out.collect("定时器" + timestamp + "触发，当前水位线是：" + ctx.timerService().currentWatermark());
                            }
                        })
                .print();

        env.execute();
    }
}
