package com.atguigu.flink.datastreamapi.transform;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 自定义物理分区
 */
public class CustomerPartTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .partitionCustom(
                        new Partitioner<Integer>() {
                            @Override
                            public int partition(Integer key, int numPartitions) {
                                // key为0发送到第0个并行子任务
                                // 并行子任务的索引从0开始
                                if (key == 0) {
                                    return 0;
                                }

                                // 余数为1发送到第1个并行子任务
                                return 1;
                            }
                        },
                        new KeySelector<Integer, Integer>() {
                            @Override
                            public Integer getKey(Integer value) throws Exception {
                                return value % 2;
                            }
                        })
                .print().setParallelism(2);


        env.execute();
    }
}
