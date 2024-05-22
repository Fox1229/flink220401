package com.atguigu.flink.watermark1;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutputStreamTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<String> result = env
                .addSource(
                        new SourceFunction<String>() {
                            @Override
                            public void run(SourceContext<String> ctx) throws Exception {
                                ctx.collectWithTimestamp("a", 1000L);
                                ctx.collectWithTimestamp("b", 2000L);
                                ctx.emitWatermark(new Watermark(1999L));
                                ctx.collectWithTimestamp("c", 1500L);
//                                ctx.collectWithTimestamp("a", 1000L);
                            }

                            @Override
                            public void cancel() {

                            }
                        }
                )
                .process(
                        new ProcessFunction<String, String>() {
                            @Override
                            public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {

                                // 数据
                                if (ctx.timestamp() < ctx.timerService().currentWatermark()) {
                                    ctx.output(
                                            new OutputTag<String>("later-event") {},
                                            "数据" + value + "迟到了，数据的事件时间是：" + ctx.timestamp() +
                                                    "，当前水位线：" + ctx.timerService().currentWatermark()
                                    );
                                } else {
                                    out.collect("数据" + value + "正常输出");
                                }
                            }
                        });

        result.print("主流");
        result.getSideOutput(new OutputTag<String>("later-event"){}).print("测输出流");

        env.execute();
    }
}
