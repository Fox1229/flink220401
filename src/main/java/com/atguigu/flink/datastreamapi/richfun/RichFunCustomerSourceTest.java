package com.atguigu.flink.datastreamapi.richfun;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class RichFunCustomerSourceTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 自定义数据源并设置并行度
        env
                .addSource(
                        new RichParallelSourceFunction<String>() {
                            @Override
                            public void run(SourceContext<String> ctx) throws Exception {

                                for (int i = 0; i < 9; i++) {
                                    ctx.collect("并行子任务 " + getRuntimeContext().getIndexOfThisSubtask() +
                                            "，发送的数据是 " + i);
                                }
                            }

                            @Override
                            public void cancel() {

                            }
                        }).setParallelism(2)
                .print().setParallelism(2);

        env.execute();
    }
}
