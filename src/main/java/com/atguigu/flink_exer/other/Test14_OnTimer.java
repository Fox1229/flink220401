package com.atguigu.flink_exer.other;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import java.sql.Timestamp;

public class Test14_OnTimer {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .socketTextStream("hadoop102", 6666)
                .keyBy(r -> 1)
                .process(new KeyedProcessFunction<Integer, String, String>() {
                    @Override
                    public void processElement(String value, KeyedProcessFunction<Integer, String, String>.Context context, Collector<String> out) throws Exception {
                        // 当前机器时间
                        long currTs = context.timerService().currentProcessingTime();
                        // 10s之后的时间
                        long tenAfterTs = currTs + 10 * 1000;
                        // 注册10s之后的定时器
                        context.timerService().registerProcessingTimeTimer(tenAfterTs);

                        out.collect(value + "到达，当前机器时间为" + new Timestamp(currTs) +
                                "，注册了机器时间为" + new Timestamp(tenAfterTs) + "的定时器");
                    }

                    public void onTimer(long timestamp, KeyedProcessFunction<Integer, String, String>.OnTimerContext context, Collector<String> out) throws Exception {
                        // 定时器触发
                        out.collect("定时器触发" + new Timestamp(timestamp) +
                                "触发，当前机器时间为" + new Timestamp(context.timerService().currentProcessingTime()));
                    }
                })
                .print();

        env.execute();
    }
}
