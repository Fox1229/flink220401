package com.atguigu.flink.lowapi;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Random;

/**
 * 统计历史奇偶数排序
 */
public class KeyProcessFunDoListSortTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(
                        new SourceFunction<Integer>() {
                            private Boolean running = true;
                            private Random random = new Random();

                            @Override
                            public void run(SourceContext<Integer> ctx) throws Exception {
                                while (running) {
                                    ctx.collect(random.nextInt(100));
                                    Thread.sleep(1000L);
                                }
                            }

                            @Override
                            public void cancel() {
                                running = false;
                            }
                        }
                ).keyBy(r -> r % 2)
                .process(
                        new KeyedProcessFunction<Integer, Integer, String>() {

                            private ListState<Integer> historyData;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                historyData = getRuntimeContext().getListState(
                                        new ListStateDescriptor<>("historyData", Types.INT)
                                );
                            }

                            @Override
                            public void processElement(Integer value, KeyedProcessFunction<Integer, Integer, String>.Context ctx, Collector<String> out) throws Exception {

                                // 如果value的key是0, 就添加到0对应的列表中
                                // 如果value的key是1, 就添加到1对应的列表中
                                // 将每条数据添加到historyData
                                historyData.add(value); // 此行代码底层实现：获取value对应的key，在historyData维护的哈希表中，判断key是否存在（不存在，创建；存在，添加到列表中）

                                // historyData没有现成的排序方法，定义新的集合，方便进行排序
                                ArrayList<Integer> integers = new ArrayList<>();
                                for (Integer ele : historyData.get()) {
                                    integers.add(ele);
                                }

                                // 降序排序
                                integers.sort((i1, i2) -> i2 - i1);

                                if (ctx.getCurrentKey() == 0) {
                                    System.out.println("偶数降序排序为: ");
                                } else {
                                    System.out.println("奇数降序排序为: ");
                                }

                                StringBuilder sb = new StringBuilder();
                                for (int i = 0; i < integers.size() - 1; i++) {
                                    sb.append(integers.get(i)).append(" -> ");
                                }
                                sb.append(integers.get(integers.size() - 1));

                                out.collect(sb.toString());
                            }
                        })
                .print();


        env.execute();
    }
}
