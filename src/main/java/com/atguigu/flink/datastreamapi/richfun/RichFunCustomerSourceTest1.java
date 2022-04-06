package com.atguigu.flink.datastreamapi.richfun;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class RichFunCustomerSourceTest1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 自定义数据源并设置并行度
        env
                .addSource(
                        new RichParallelSourceFunction<Integer>() {
                            @Override
                            public void run(SourceContext<Integer> ctx) throws Exception {

                                for (int i = 1; i < 9; i++) {
                                    if(i % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
                                        ctx.collect(i);
                                    }
                                }
                            }

                            @Override
                            public void cancel() {

                            }
                        }).setParallelism(2)
                // 将数据发送到下游所有并行子任务
                //.rebalance()
                // 将数据发送到下游某些并行子任务
                .rescale()
                .print().setParallelism(4);

        env.execute();
    }
}
