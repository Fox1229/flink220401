package com.atguigu.flink_exer.other;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Test24_LaterDate {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 侧输出流标签
        OutputTag<String> laterData = new OutputTag<>("laterData", TypeInformation.of(String.class));

        SingleOutputStreamOperator<String> resStream = env
                .addSource(new SourceFunction<String>() {
                    @Override
                    public void run(SourceContext<String> ctx) throws Exception {
                        ctx.collectWithTimestamp("a", 1000L);
                        ctx.collectWithTimestamp("b", 2000L);
                        ctx.emitWatermark(new Watermark(1999L));
                        ctx.collectWithTimestamp("c", 1000L);
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                        // 迟到数据
                        if (ctx.timestamp() < ctx.timerService().currentWatermark()) {
                            // 声明侧输出流标签
                            ctx.output(
                                    laterData,
                                    value + "迟到了，当前事件时间" + ctx.timestamp() + "，当前水位线" + ctx.timerService().currentWatermark()
                            );
                        } else {
                            // 主流数据
                            out.collect(value);
                        }


                    }
                });

        resStream.print("主流>>>");
        resStream.getSideOutput(laterData).print("侧输出流>>>");


        env.execute();
    }
}
