package com.atguigu.flink.homework;

import com.atguigu.flink.pojo.ProductViewCountPerWindow;
import com.atguigu.flink.pojo.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * 1. 实现处理时间滑动窗口。实现全窗口函数。
 */
public class KeyedProcessFunImplSlidingWindow1 {

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
                .process(new TopN(3))
                .print();

        env.execute();
    }

    public static class TopN extends KeyedProcessFunction<Long, ProductViewCountPerWindow, String> {

        private final Integer takeSize;
        private ListState<ProductViewCountPerWindow> listState;

        public TopN(Integer takeSize) {
            this.takeSize = takeSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(
                    new ListStateDescriptor<ProductViewCountPerWindow>("listState", Types.POJO(ProductViewCountPerWindow.class))
            );
        }

        @Override
        public void processElement(ProductViewCountPerWindow value, KeyedProcessFunction<Long, ProductViewCountPerWindow, String>.Context ctx, Collector<String> out) throws Exception {

            listState.add(value);

            ctx.timerService().registerEventTimeTimer(value.windowEndTime + 1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            ArrayList<ProductViewCountPerWindow> list = new ArrayList<>();
            for (ProductViewCountPerWindow tmp : listState.get()) {
                list.add(tmp);
            }

            // 降序排序
            list.sort((t1, t2) -> (int) (t2.count - t1.count));

            // 取出Top3
            StringBuilder sb = new StringBuilder();
            sb.append("窗口结束时间").append(new Timestamp(timestamp)).append("\n");
            sb.append("=======================\n");
            for (int i = 0; i < takeSize; i++) {
                ProductViewCountPerWindow prod = list.get(i);
                sb.append(i + 1).append(": 商品: ").append(prod.productId).append(", 访问次数").append(prod.count).append("\n");
            }
            sb.append("=======================\n");

            out.collect(sb.toString());
        }
    }

    public static class MySlidingWindow extends KeyedProcessFunction<String, UserBehavior, ProductViewCountPerWindow> {

        private final Long windowSize;
        private final Long slidingSize;
        // <窗口开始时间, 数据>
        private MapState<Long, List<UserBehavior>> mapState;

        public MySlidingWindow(Long windowSize, Long slidingSize) {
            this.windowSize = windowSize;
            this.slidingSize = slidingSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            mapState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<Long, List<UserBehavior>>("mapState", Types.LONG, Types.LIST(Types.POJO(UserBehavior.class)))
            );
        }

        @Override
        public void processElement(UserBehavior action, KeyedProcessFunction<String, UserBehavior, ProductViewCountPerWindow>.Context ctx, Collector<ProductViewCountPerWindow> out) throws Exception {

            // 获取数据的第一个窗口的开始时间
            long firstStartTime = action.ts - action.ts % slidingSize;

            // 获取数据的所有窗口开始时间
            ArrayList<Long> windowStartTime = new ArrayList<>();
            for (long i = firstStartTime; i > action.ts - windowSize; i -= slidingSize) {
                windowStartTime.add(i);
            }

            // 添加数据
            for (Long startTime : windowStartTime) {
                if (!mapState.contains(startTime)) {
                    ArrayList<UserBehavior> list = new ArrayList<>();
                    list.add(action);
                    mapState.put(startTime, list);
                } else {
                    mapState.get(startTime).add(action);
                }
            }

            // 注册定时器
            for (Long startTime : windowStartTime) {
                ctx.timerService().registerEventTimeTimer(startTime + windowSize - 1L);
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, UserBehavior, ProductViewCountPerWindow>.OnTimerContext ctx, Collector<ProductViewCountPerWindow> out) throws Exception {

            String productId = ctx.getCurrentKey();
            long endTime = timestamp + 1L;
            long startTime = endTime - windowSize;
            long count = (long) mapState.get(startTime).size();

            out.collect(
                    new ProductViewCountPerWindow(productId, count, startTime, endTime)
            );
        }
    }
}
