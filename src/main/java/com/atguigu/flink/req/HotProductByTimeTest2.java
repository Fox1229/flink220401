package com.atguigu.flink.req;

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

public class HotProductByTimeTest2 {

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
                // 计算每个窗口中每个商品的点击次数
                .aggregate(new ProductAcc(), new ProductProcess())
                // 按照相同结束时间分组。按照访问次数排序或取top3
                .keyBy(r -> r.windowEndTime)
                .process(new ProductCountTopN(3))
                .print();

        env.execute();
    }

    public static class ProductCountTopN extends KeyedProcessFunction<Long, ProductViewCountPerWindow, String> {

        private final Integer productCnt;
        private ListState<ProductViewCountPerWindow> listState;

        public ProductCountTopN(Integer productCnt) {
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

            // 将数据保存到list中维护状态
            listState.add(value);

            // 我们注册了一个窗口结束时间+1毫秒的定时器
            // 触发定时器的时候，能保证windowEndTime所对应的统计信息都到齐了吗？
            ctx.timerService().registerEventTimeTimer(value.windowEndTime + 1L);
        }

        /**
         * 定时触发
         */
        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, ProductViewCountPerWindow, String>.OnTimerContext ctx, Collector<String> out) throws Exception {

            ArrayList<ProductViewCountPerWindow> list = new ArrayList<>();
            for (ProductViewCountPerWindow tmp : listState.get()) {
                list.add(tmp);
            }

            // 按照访问次数降序排序
            list.sort((t1, t2) -> (int) (t2.count - t1.count));

            // 当集合中的元素个数达到排序数量时进行排序
            if (list.size() >= productCnt) {
                StringBuilder sb = new StringBuilder();
                sb.append("窗口结束时间").append(new Timestamp(ctx.getCurrentKey())).append("\n");
                sb.append("==========================\n");
                for (int i = 0; i < productCnt; i++) {
                    ProductViewCountPerWindow tmp = list.get(i);
                    sb
                            .append(i + 1)
                            .append("商品id: ").append(tmp.productId)
                            .append(", 访问次数: ").append(tmp.count)
                            .append("\n");
                }
                sb.append("==========================\n");

                // 返回结果
                out.collect(sb.toString());
            }
        }
    }

    public static class ProductProcess extends ProcessWindowFunction<Long, ProductViewCountPerWindow, String, TimeWindow> {
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
