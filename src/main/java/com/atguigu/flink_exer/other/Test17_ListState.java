package com.atguigu.flink_exer.other;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import java.util.ArrayList;
import java.util.Random;

public class Test17_ListState {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new SourceFunction<Tuple2<Integer, Long>>() {
                    private boolean flag = true;

                    @Override
                    public void run(SourceContext<Tuple2<Integer, Long>> ctx) throws Exception {
                        while (flag) {
                            Random random = new Random();

                            ctx.collect(
                                    Tuple2.of(
                                            random.nextInt(1000),
                                            System.currentTimeMillis()
                                    )
                            );

                            Thread.sleep(100L);
                        }
                    }

                    @Override
                    public void cancel() {
                        flag = false;
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<Integer, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner((t, l) -> t.f1))
                .keyBy(t -> t.f0 % 2)
                .process(new MyTopN())
                .print();

        env.execute();
    }

    public static class MyTopN extends KeyedProcessFunction<Integer, Tuple2<Integer, Long>, String> {
        private ListState<Tuple2<Integer, Long>> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ListStateDescriptor<Tuple2<Integer, Long>> listStateDesc = new ListStateDescriptor<>("listStateDesc", Types.TUPLE(Types.INT, Types.LONG));
            listState = getRuntimeContext().getListState(listStateDesc);
        }

        @Override
        public void processElement(Tuple2<Integer, Long> value, KeyedProcessFunction<Integer, Tuple2<Integer, Long>, String>.Context ctx, Collector<String> out) throws Exception {
            // 将数据保存到状态中
            listState.add(value);

            // 读取历史数据
            ArrayList<Integer> list = new ArrayList<>();
            for (Tuple2<Integer, Long> tuple2 : listState.get()) {
                list.add(tuple2.f0);
            }
            // 降序排序
            list.sort((i1, i2) -> i2 - i1);

            // 打印
            StringBuilder sb = new StringBuilder();
            Integer currentKey = ctx.getCurrentKey();
            String flag;
            if (currentKey == 0) {
                flag = "偶数";
            } else {
                flag = "奇数";
            }
            sb.append(flag).append(" ");
            for (int i = 0; i < list.size() - 1; i++) {
                sb.append(list.get(i)).append("=>");
            }
            sb.append(list.get(list.size() - 1));

            out.collect(String.valueOf(sb));
        }
    }
}
