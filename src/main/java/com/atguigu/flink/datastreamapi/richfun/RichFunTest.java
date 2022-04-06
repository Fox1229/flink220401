package com.atguigu.flink.datastreamapi.richfun;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RichFunTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .keyBy(ele -> ele % 3)
                .reduce(
                        new RichReduceFunction<Integer>() {
                            @Override
                            public Integer reduce(Integer value1, Integer value2) throws Exception {
                                return value1 + value2;
                            }
                        }).setParallelism(2)
                .map(
                        new RichMapFunction<Integer, String>() {
                            @Override
                            public void open(Configuration parameters) throws Exception {
                                System.out.println("并行任务索引 " + getRuntimeContext().getIndexOfThisSubtask() + " 开始执行");
                            }

                            @Override
                            public String map(Integer value) throws Exception {
                                return "并行子任务的索引为：" + getRuntimeContext().getIndexOfThisSubtask() +
                                        "，输出的数据为：" + value;
                            }

                            @Override
                            public void close() throws Exception {
                                System.out.println("并行任务索引 " + getRuntimeContext().getIndexOfThisSubtask() + " 停止执行");
                            }
                        }).setParallelism(2)
                .print().setParallelism(2);

        env.execute();
    }
}
