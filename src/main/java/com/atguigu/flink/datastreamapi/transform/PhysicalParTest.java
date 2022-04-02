package com.atguigu.flink.datastreamapi.transform;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PhysicalParTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /*env
                .fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .shuffle()
                .print("随机发送").setParallelism(2);*/

        /*env
                .fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .rebalance()
                .print("轮询发送").setParallelism(4);*/

        /*env
                .fromElements(1, 2, 3, 4)
                .broadcast()
                .print("广播发送").setParallelism(2);*/

        /*env
                .fromElements(1, 2, 3, 4)
                .global()
                .print("global发送").setParallelism(2);*/

        /*env
                .fromElements(1, 2, 3, 4, 5, 6)
                .rescale()
                .print("rescale发送").setParallelism(4);*/

        /*env
                .fromElements(1, 2, 3, 4, 5, 6)
                .partitionCustom()
                .print("rescale发送").setParallelism(4);*/

        env.execute();
    }
}
