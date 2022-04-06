package com.atguigu.flink.lowapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ProcessFunTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements("write", "black", "gray")
                .keyBy(r -> true)
                .process(
                        new KeyedProcessFunction<Boolean, String, String>() {
                            @Override
                            public void processElement(String value, KeyedProcessFunction<Boolean, String, String>.Context ctx, Collector<String> out) throws Exception {
                                if ("write".equals(value)) {
                                    out.collect(value);
                                } else {
                                    out.collect(value);
                                    out.collect(value);
                                }
                            }
                        })
                .print();

        env.execute();
    }
}
