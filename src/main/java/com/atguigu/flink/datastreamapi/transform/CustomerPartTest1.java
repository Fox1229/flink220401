package com.atguigu.flink.datastreamapi.transform;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CustomerPartTest1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /*env
                .fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .keyBy(ele -> ele % 3)
                .print("keyBy").setParallelism(3);*/

        // 自定义分区
        env
                .fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .partitionCustom(
                        new Partitioner<Integer>() {
                            @Override
                            public int partition(Integer key, int numPartitions) {
                                return key;
                            }
                        },
                        new KeySelector<Integer, Integer>() {
                            @Override
                            public Integer getKey(Integer value) throws Exception {
                                return value % 3;
                            }
                        })
                .print("MyPart").setParallelism(3);

        env.execute();
    }
}
