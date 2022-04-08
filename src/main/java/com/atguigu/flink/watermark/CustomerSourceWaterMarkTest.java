package com.atguigu.flink.watermark;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

public class CustomerSourceWaterMarkTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(
                        new SourceFunction<String>() {
                            @Override
                            public void run(SourceContext<String> ctx) throws Exception {
                                ctx.emitWatermark(new Watermark(-100 * 1000L));
                                ctx.collectWithTimestamp("hello", 1000L);
                                ctx.emitWatermark(new Watermark(100 * 1000L));
                            }

                            @Override
                            public void cancel() {

                            }
                        }
                )
                .keyBy(r -> 1)
                .process(
                        new KeyedProcessFunction<Integer, String, String>() {
                            @Override
                            public void processElement(String value, KeyedProcessFunction<Integer, String, String>.Context ctx, Collector<String> out) throws Exception {

                                out.collect("数据" + value + "到达，时间戳为：" + ctx.timestamp());
                                ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 10 * 1000L);
                                out.collect("注册了" + (ctx.timestamp() + 10 * 1000L) + "的定时器");
                            }

                            @Override
                            public void onTimer(long timestamp, KeyedProcessFunction<Integer, String, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                                out.collect("定时器" + timestamp + "触发，当前process算子的水位线是：" + ctx.timerService().currentWatermark());
                            }
                        })
                .print();

        env.execute();
    }
}
