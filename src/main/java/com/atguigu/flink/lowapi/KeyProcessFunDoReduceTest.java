package com.atguigu.flink.lowapi;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

public class KeyProcessFunDoReduceTest {

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
                .keyBy(r -> true)
                .process(new InStatistic())
                .print();

        env.execute();
    }

    public static class InStatistic extends KeyedProcessFunction<Boolean, Integer, String> {

        // 声明状态变量
        // <最小值, 最大值, 总和, 数量, 平均值>
        private ValueState<Tuple5<Integer, Integer, Integer, Integer, Integer>> accumulator;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化状态变量
            accumulator = getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple5<Integer, Integer, Integer, Integer, Integer>>(
                            "accumulator",
                            Types.TUPLE(Types.INT, Types.INT, Types.INT, Types.INT, Types.INT)
                    )
            );
        }

        @Override
        public void processElement(Integer value, KeyedProcessFunction<Boolean, Integer, String>.Context ctx, Collector<String> out) throws Exception {

            // 如果状态变量为空，说明到达的是第一条数据
            if (accumulator.value() == null) {
                // 直接更新状态变量
                accumulator.update(Tuple5.of(value, value, value, 1, value));
            } else {
                // 后面数据到达，和状态变量中的累加器进行聚合，然后写会状态变量
                Tuple5<Integer, Integer, Integer, Integer, Integer> tmp = accumulator.value();
                accumulator.update(
                        Tuple5.of(
                                Math.min(value, tmp.f0),        // 计算最小值
                                Math.max(tmp.f1, value),        // 计算最大值
                                value + tmp.f2,                 // 计算总和
                                tmp.f3 + 1,                     // 总条数 + 1
                                (value + tmp.f2) / (tmp.f3 + 1) // 计算平均值
                        )
                );
            }

            // 采集
            out.collect(
                    "最小值：" + accumulator.value().f0 +
                            ", 最大值：" + accumulator.value().f1 +
                            ", 总和：" + accumulator.value().f2 +
                            ", 总条数：" + accumulator.value().f3 +
                            ", 平均值：" + accumulator.value().f4
            );
        }
    }
}
