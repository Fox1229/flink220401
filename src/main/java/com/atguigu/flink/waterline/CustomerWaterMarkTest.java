package com.atguigu.flink.waterline;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 自定义水位线
 */
public class CustomerWaterMarkTest {

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
                                return Tuple2.of(fields[0], Long.parseLong(fields[1]) * 1000L);
                            }
                        }
                )
                .assignTimestampsAndWatermarks(
                        new WatermarkStrategy<Tuple2<String, Long>>() {

                            @Override
                            public TimestampAssigner<Tuple2<String, Long>> createTimestampAssigner(TimestampAssignerSupplier.Context context) {

                                return new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                        // 指定时间字段
                                        return element.f1;
                                    }
                                };
                            }

                            @Override
                            public WatermarkGenerator<Tuple2<String, Long>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {

                                return new WatermarkGenerator<Tuple2<String, Long>>() {

                                    // 最大延迟时间
                                    private long bound = 5000L;
                                    // 每次观察到的最大事件时间
                                    private long maxTs = Long.MAX_VALUE + bound + 1L;

                                    /**
                                     * 每一条数据触发调用，更新观察到的最大事件时间
                                     */
                                    @Override
                                    public void onEvent(Tuple2<String, Long> event, long eventTimestamp, WatermarkOutput output) {
                                        maxTs = Math.max(event.f1, maxTs);

                                        // 针对特殊的数据发送水位线
                                        if ("hello".equals(event.f0)) {
                                            output.emitWatermark(new Watermark(Long.MAX_VALUE));
                                        }
                                    }

                                    /**
                                     * 周期性调用，默认每隔200ms调用一次
                                     * 周期性插入水位线
                                     */
                                    @Override
                                    public void onPeriodicEmit(WatermarkOutput output) {
                                        output.emitWatermark(new Watermark(maxTs - bound - 1L));
                                    }
                                };
                            }
                        }
                )
                .keyBy(r -> r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(
                        new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                            @Override
                            public void process(String s, ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>.Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {

                                long count = elements.spliterator().getExactSizeIfKnown();
                                out.collect("key:" + s + ", 窗口：" + context.window().getStart() + "~" + context.window().getEnd() +
                                        "包含" + count + "条数据");
                            }
                        })
                .print();

        env.execute();
    }
}
