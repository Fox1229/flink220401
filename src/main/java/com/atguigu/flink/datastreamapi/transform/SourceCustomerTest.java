package com.atguigu.flink.datastreamapi.transform;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceCustomerTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ClickSource()).print();

        env.execute();
    }
}
