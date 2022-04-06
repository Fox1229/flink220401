package com.atguigu.flink.lowapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class keyProcessFunTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .socketTextStream("hadoop102", 9999)
                .keyBy(r -> 1)
                .process(
                        new KeyedProcessFunction<Integer, String, String>() {
                            @Override
                            public void processElement(String value, KeyedProcessFunction<Integer, String, String>.Context ctx, Collector<String> out) throws Exception {
                                // 数据到达时间
                                long time = ctx.timerService().currentProcessingTime();
                                // 注册10s后的定时器
                                long tenSecondsLater = time + 10 * 1000L;
                                ctx.timerService().registerProcessingTimeTimer(tenSecondsLater);

                                // 注册20s后的定时器
                                long twentySecondsLater = time + 20 * 1000L;
                                ctx.timerService().registerProcessingTimeTimer(twentySecondsLater);

                                System.out.println(
                                        "数据" + value
                                                + ", 到达时间 " + new Timestamp(time)
                                                + "，注册定时器1的时间为 " + new Timestamp(tenSecondsLater)
                                                + "，注册定时器2的时间为 " + new Timestamp(twentySecondsLater));
                            }

                            @Override
                            public void onTimer(long timestamp, KeyedProcessFunction<Integer, String, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                                out.collect("定时器 " + new Timestamp(timestamp) + "触发, 执行时间为 " +
                                        new Timestamp(ctx.timerService().currentProcessingTime()));
                            }
                        }).print();

        env.execute();
    }
}
