package com.atguigu.flink.watermark1;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 双流join
 */
public class StreamJoinTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        SingleOutputStreamOperator<Tuple2<String, Long>> stream1 = env
                .socketTextStream("hadoop102", 6666)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] fields = value.split(" ");
                        return Tuple2.of(fields[0], Long.parseLong(fields[1]) * 1000L);
                    }
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                        return element.f1;
                                    }
                                })
                );

        SingleOutputStreamOperator<Tuple2<String, Long>> stream2 = env
                .socketTextStream("hadoop102", 8888)
                .map(
                        new MapFunction<String, Tuple2<String, Long>>() {
                            @Override
                            public Tuple2<String, Long> map(String value) throws Exception {
                                String[] fields = value.split(" ");
                                return Tuple2.of(fields[0], Long.parseLong(fields[1]) * 1000L);
                            }
                        })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                        return element.f1;
                                    }
                                })
                );

        // 合流
        stream1
                .union(stream2)
                .process(
                        new ProcessFunction<Tuple2<String, Long>, String>() {
                            @Override
                            public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {

                                out.collect("数据" + value + "到达，当前水位线是：" + ctx.timerService().currentWatermark());
                            }
                        })
                .print();

        env.execute();
    }
}
