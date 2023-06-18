package com.atguigu.flink.watermark1;

import com.atguigu.flink.pojo.ProductViewCountPerWindow;
import com.atguigu.flink.pojo.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;

public class HotProductByTimeTest2_2 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .readTextFile("D:\\developer\\idea workspace\\flink220401\\data\\UserBehavior.csv")
                .map(
                        new MapFunction<String, UserBehavior>() {
                            @Override
                            public UserBehavior map(String in) throws Exception {
                                String[] fields = in.split(",");
                                return new UserBehavior(fields[0], fields[1], fields[2], fields[3], Long.parseLong(fields[4]) * 1000);
                            }
                        }
                )
                // 过滤数据
                .filter(r -> "pv".equals(r.type))
                // 设置水位线
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
                // 将数据分配到不同的slot
                .keyBy(r -> r.productId)
                // 开窗
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                // 聚合
                .aggregate(new MyAgg(), new MyWindow())
                // 将相同窗口的多个商品聚合
                .keyBy(r -> r.windowEndTime)
                .process(new ProductTopN(3))
                .print();

        env.execute();
    }

    public static class ProductTopN extends KeyedProcessFunction<Long, ProductViewCountPerWindow, String> {

        private long n;
        private ListState<ProductViewCountPerWindow> listState;

        public ProductTopN(long n) {
            this.n = n;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(
                    new ListStateDescriptor<ProductViewCountPerWindow>(
                            "listState", Types.POJO(ProductViewCountPerWindow.class)
                    )
            );
        }

        @Override
        public void processElement(ProductViewCountPerWindow value, Context context, Collector<String> out) throws Exception {

            // 将到来的数据添加到状态中
            listState.add(value);
            // 注册定时器
            context.timerService().registerEventTimeTimer(value.windowEndTime + 1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            // 获取当前窗口的保存的所有数据，添加到list集合，方便排序
            ArrayList<ProductViewCountPerWindow> list = new ArrayList<>();
            Iterable<ProductViewCountPerWindow> productViewCountPerWindows = listState.get();
            for (ProductViewCountPerWindow productViewCountPerWindow : productViewCountPerWindows) {
                list.add(productViewCountPerWindow);
            }

            // 按照访问次数降序排序
            list.sort((t1, t2) -> (int) (t2.count - t1.count));

            // 满足需要的数据条数
            StringBuilder sb = new StringBuilder();
            sb
                    .append("窗口结束时间")
                    .append(new Timestamp(timestamp))
                    .append("\n=======================================\n");
            for (int i = 0; i < n; i++) {
                ProductViewCountPerWindow productViewCount = list.get(i);
                sb.append(i + 1).append(", 商品：").append(productViewCount.productId).append(", 访问次数：").append(productViewCount.count).append("\n");
            }
            sb.append("=======================================\n");
            out.collect(sb.toString());
        }
    }

    public static class MyWindow extends ProcessWindowFunction<Long, ProductViewCountPerWindow, String, TimeWindow> {

        @Override
        public void process(String s, Context context, Iterable<Long> elements, Collector<ProductViewCountPerWindow> out) throws Exception {
            out.collect(
                    new ProductViewCountPerWindow(
                            s,
                            elements.iterator().next(),
                            context.window().getStart(),
                            context.window().getEnd()
                    )
            );
        }
    }

    public static class MyAgg implements AggregateFunction<UserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }
}
