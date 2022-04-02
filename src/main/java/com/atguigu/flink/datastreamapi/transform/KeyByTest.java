package com.atguigu.flink.datastreamapi.transform;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyByTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .keyBy(r -> r % 3)
                .sum(0).setParallelism(4)
                .print().setParallelism(4);

        env.execute();
    }
}
