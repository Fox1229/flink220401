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

public class KeyProcessFunDoReduceNTimeTest1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(
                        // 模拟数据
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

    /**
     * 计算最小值，最大值，总和，总条数，平均值
     */
    public static class InStatistic extends KeyedProcessFunction<Boolean, Integer, String> {

        // 定义状态
        private ValueState<Tuple5<Integer, Integer, Integer, Integer, Integer>> accumulator;

        // 初始化状态
        @Override
        public void open(Configuration parameters) throws Exception {
            accumulator = getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple5<Integer, Integer, Integer, Integer, Integer>>(
                            "accumulator",
                            Types.TUPLE(Types.INT, Types.INT, Types.INT, Types.INT, Types.INT)
                    )
            );
        }

        @Override
        public void processElement(Integer value, KeyedProcessFunction<Boolean, Integer, String>.Context ctx, Collector<String> out) throws Exception {

            if (accumulator.value() == null) {
                // 第一条启动, 将第一条数据加入状态
                accumulator.update(new Tuple5<>(value, value, value, 1, value));
            } else {
                // 非首次启动，更新原有数据
                Tuple5<Integer, Integer, Integer, Integer, Integer> tmp = accumulator.value();
                accumulator.update(
                        new Tuple5<Integer, Integer, Integer, Integer, Integer>(
                                Math.min(value, tmp.f0),
                                Math.max(value, tmp.f1),
                                value + tmp.f2,
                                tmp.f3 + 1,
                                (value + tmp.f2) / (tmp.f3 + 1)
                        )
                );
            }

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
