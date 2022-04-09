package com.atguigu.flink.watermark1;

import com.atguigu.flink.pojo.ProductViewCountPerWindow;
import com.atguigu.flink.pojo.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
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

public class HotProductByTimeTest2_1 {

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
                        })
                .filter(r -> "pv".equals(r.type))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserBehavior>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                                    @Override
                                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                        return element.ts;
                                    }
                                })
                )
                .keyBy(r -> r.productId)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                // 计算每个窗口，每个商品的访问次数
                .aggregate(new ProductAcc(), new ProductFun())
                // 按照相同结束时间分组，计算每个窗口商品访问排行
                .keyBy(r -> r.windowEndTime)
                .process(new ProductFunTopN(3))
                .print();

        env.execute();
    }

    public static class ProductFunTopN extends KeyedProcessFunction<Long, ProductViewCountPerWindow, String> {

        private final Integer productCnt;
        private ListState<ProductViewCountPerWindow> listState;

        public ProductFunTopN(Integer productCnt) {
            this.productCnt = productCnt;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(
                    new ListStateDescriptor<ProductViewCountPerWindow>("listState", Types.POJO(ProductViewCountPerWindow.class))
            );
        }

        @Override
        public void processElement(ProductViewCountPerWindow value, KeyedProcessFunction<Long, ProductViewCountPerWindow, String>.Context ctx, Collector<String> out) throws Exception {

            // 将每条数据添加到listState维护
            listState.add(value);

            // 设置定时器
            ctx.timerService().registerEventTimeTimer(value.windowEndTime + 1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            ArrayList<ProductViewCountPerWindow> list = new ArrayList<>();
            for (ProductViewCountPerWindow tmp : listState.get()) {
                list.add(tmp);
            }

            // 降序排序
            list.sort((t1, t2) -> (int)(t2.count - t1.count));

            // 取出Top3
            StringBuilder sb = new StringBuilder();
            sb.append("窗口结束时间").append(new Timestamp(timestamp)).append("\n");
            sb.append("=======================\n");
            for (int i = 0; i < productCnt; i++) {
                ProductViewCountPerWindow prod = list.get(i);
                sb.append(i + 1).append(": 商品: ").append(prod.productId).append(", 访问次数").append(prod.count).append("\n");
            }
            sb.append("=======================\n");

            out.collect(sb.toString());
        }
    }

    public static class ProductFun extends ProcessWindowFunction<Long, ProductViewCountPerWindow, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<Long> elements, Collector<ProductViewCountPerWindow> out) throws Exception {
            out.collect(
                    new ProductViewCountPerWindow(s, elements.iterator().next(), context.window().getStart(), context.window().getEnd())
            );
        }
    }

    public static class ProductAcc implements AggregateFunction<UserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1L;
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
