package com.atguigu.flink.lowapi;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.protocol.types.Field;

import java.util.Random;

public class KeyProcessFunDoReduceTest2 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new SourceFunction<Integer>() {

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
                })
                .keyBy(r -> 1)
                .process(
                        new KeyedProcessFunction<Integer, Integer, InStatistic>() {

                            // 定义状态
                            private ValueState<InStatistic> accumulator;
                            // 标识符
                            private ValueState<Boolean> flg;

                            // 初始化状态
                            @Override
                            public void open(Configuration parameters) throws Exception {
                                accumulator = getRuntimeContext().getState(
                                        new ValueStateDescriptor<InStatistic>("accumulator", Types.POJO(InStatistic.class)
                                        )
                                );
                                flg = getRuntimeContext().getState(
                                        new ValueStateDescriptor<Boolean>("flg", Types.BOOLEAN)
                                );
                            }

                            @Override
                            public void processElement(Integer value, KeyedProcessFunction<Integer, Integer, InStatistic>.Context ctx, Collector<InStatistic> out) throws Exception {
                                if (accumulator.value() == null) {
                                    // 第一条数据，直接插入
                                    accumulator.update(
                                            new InStatistic(value, value, value, 1, value)
                                    );
                                } else {
                                    // 非第一条数据，更新原有数据
                                    InStatistic tmp = accumulator.value();
                                    accumulator.update(
                                            new InStatistic(
                                                    Math.min(value, tmp.min),
                                                    Math.max(value, tmp.max),
                                                    value + tmp.sum,
                                                    tmp.cnt + 1,
                                                    (value + tmp.sum) / (tmp.cnt + 1)
                                            )
                                    );
                                }

                                // 打印每条数据
                                //out.collect(accumulator.value());

                                // 定时器为null，注册定时器
                                if(flg.value() == null) {
                                    long currentProcessingTime = ctx.timerService().currentProcessingTime();
                                    long tenSecondsLater = currentProcessingTime + 5 * 1000L;
                                    ctx.timerService().registerProcessingTimeTimer(tenSecondsLater);
                                    // 更新定时器状态
                                    flg.update(true);
                                }
                            }

                            @Override
                            public void onTimer(long timestamp, KeyedProcessFunction<Integer, Integer, InStatistic>.OnTimerContext ctx, Collector<InStatistic> out) throws Exception {
                                // 周期性统计
                                out.collect(accumulator.value());
                                // 清空定时器
                                flg.clear();
                            }
                        })
                .print();


        env.execute();
    }

    public static class InStatistic {

        public Integer min;
        public Integer max;
        public Integer sum;
        public Integer cnt;
        public Integer avg;

        public InStatistic() {
        }

        public InStatistic(Integer min, Integer max, Integer sum, Integer cnt, Integer avg) {
            this.min = min;
            this.max = max;
            this.sum = sum;
            this.cnt = cnt;
            this.avg = avg;
        }

        @Override
        public String toString() {
            return "InStatistic{" +
                    "min=" + min +
                    ", max=" + max +
                    ", sum=" + sum +
                    ", cnt=" + cnt +
                    ", avg=" + avg +
                    '}';
        }
    }
}
