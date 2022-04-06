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
 * 奇偶数排序
 */
public class KeyProcessFunDoListSortTest1 {

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
                                    Thread.sleep(100L);
                                }
                            }

                            @Override
                            public void cancel() {
                                running = false;
                            }
                        }
                )
                .keyBy(r -> r % 2)
                .process(
                        new KeyedProcessFunction<Integer, Integer, String>() {

                            // 定义状态
                            private ListState<Integer> listState;

                            // 初始化状态
                            @Override
                            public void open(Configuration parameters) throws Exception {
                                listState = getRuntimeContext().getListState(
                                        new ListStateDescriptor<Integer>("listState", Types.INT)
                                );
                            }

                            @Override
                            public void processElement(Integer value, KeyedProcessFunction<Integer, Integer, String>.Context ctx, Collector<String> out) throws Exception {

                                // 添加每条数据到listState
                                listState.add(value);

                                // 定义集合，方便排序
                                ArrayList<Integer> integers = new ArrayList<>();
                                for (Integer num : listState.get()) {
                                    integers.add(num);
                                }

                                // 升序排序
                                integers.sort((i1, i2) -> i1 - i2);

                                // 根据传入的key判断奇偶
                                if(ctx.getCurrentKey() == 0) {
                                    System.out.println("偶数排序：");
                                } else {
                                    System.out.println("奇数排序：");
                                }

                                // 拼接字符串
                                StringBuilder sb = new StringBuilder();
                                for (int i = 0; i < integers.size() - 1; i++) {
                                    sb.append(integers.get(i)).append(" -> ");
                                }
                                sb.append(integers.get(integers.size() - 1));

                                // 每条数据写出
                                 out.collect(sb.toString());
                            }
                        })
                .print();

        env.execute();
    }
}
