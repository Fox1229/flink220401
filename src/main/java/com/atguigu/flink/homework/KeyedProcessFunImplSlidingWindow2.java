package com.atguigu.flink.homework;

import com.atguigu.flink.pojo.ProductViewCountPerWindow;
import com.atguigu.flink.pojo.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class KeyedProcessFunImplSlidingWindow2 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .readTextFile("D:\\developer\\idea workspace\\flink220401\\data\\UserBehavior.csv")
                .map(
                        new MapFunction<String, UserBehavior>() {
                            @Override
                            public UserBehavior map(String value) throws Exception {
                                String[] fields = value.split(",");
                                return new UserBehavior(fields[0], fields[1], fields[2], fields[3], Long.parseLong(fields[4]) * 1000L);
                            }
                        }
                )
                .filter(r -> r.type.equals("pv"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserBehavior>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<UserBehavior>() {
                                            @Override
                                            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                                return element.ts;
                                            }
                                        }
                                )
                )
                .keyBy(r -> r.productId)
                .process(new MySlidingWindow(60 * 60 * 1000L, 5 * 60 * 1000L))
                .keyBy(r -> r.windowEndTime)
                .process(new KeyedProcessFunImplSlidingWindow.TopN(3))
                .print();

        env.execute();
    }

    public static class MySlidingWindow extends KeyedProcessFunction<String, UserBehavior, ProductViewCountPerWindow> {

        private Long windowSize;
        private Long slidingStep;
        // <窗口开始时间, count>
        private MapState<Long, Long> mapState;

        public MySlidingWindow(Long windowSize, Long slidingStep) {
            this.windowSize = windowSize;
            this.slidingStep = slidingStep;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            mapState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<Long, Long>("mapState", Types.LONG, Types.LONG)
            );
        }

        @Override
        public void processElement(UserBehavior action, KeyedProcessFunction<String, UserBehavior, ProductViewCountPerWindow>.Context ctx, Collector<ProductViewCountPerWindow> out) throws Exception {

            // 获取数据的第一个窗口开始时间
            long firstStartTime = action.ts - action.ts % slidingStep;

            // 获取数据的所有窗口开始时间
            ArrayList<Long> list = new ArrayList<>();
            for (long i = firstStartTime; i > action.ts - windowSize; i -= slidingStep) {
                list.add(i);
            }

            for (Long startTime : list) {
                if (!mapState.contains(startTime)) {
                    // 首次添加
                    mapState.put(startTime, 1L);
                } else {
                    mapState.put(startTime, mapState.get(startTime) + 1L);
                }
            }

            // 注册定时器
            for (Long startTime : list) {
                ctx.timerService().registerEventTimeTimer(startTime + windowSize - 1L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<ProductViewCountPerWindow> out) throws Exception {
            String productId = ctx.getCurrentKey();
            long endTime = timestamp + 1;
            long startTime = endTime - windowSize;
            Long count = mapState.get(startTime);
            out.collect(new ProductViewCountPerWindow(productId, count, startTime, endTime));
        }
    }
}
