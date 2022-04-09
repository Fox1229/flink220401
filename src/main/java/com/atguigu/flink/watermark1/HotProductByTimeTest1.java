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

/**
 * 方式二
 *      缺点：每个窗口打印多次，数据量多
 */
public class HotProductByTimeTest1 {

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
                                .<UserBehavior>forMonotonousTimestamps() // 默认延迟为0
                                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                                    @Override
                                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                        return element.ts;
                                    }
                                })
                )
                // 将数据发送到不同的任务插槽
                .keyBy(r -> r.productId)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                // 计算每个商品在每个窗口的访问次数
                .aggregate(new ProductAcc(), new ProductPv())
                // 按照窗口结束时间分组
                // 每个分组是相同窗口的不同商品的统计信息
                .keyBy(r -> r.windowEndTime)
                .process(new ProductPvTopN(3L))
                .print();

        env.execute();
    }

    public static class ProductAcc implements AggregateFunction<UserBehavior, Long, Long> {
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

    public static class ProductPv extends ProcessWindowFunction<Long, ProductViewCountPerWindow, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<Long> elements, Collector<ProductViewCountPerWindow> out) throws Exception {
            out.collect(
                    new ProductViewCountPerWindow(s, elements.iterator().next(), context.window().getStart(), context.window().getEnd())
            );
        }
    }

    public static class ProductPvTopN extends KeyedProcessFunction<Long, ProductViewCountPerWindow, String> {

        private Long productCnt;
        // 记录添加的商品
        private ListState<ProductViewCountPerWindow> listState;

        public ProductPvTopN(Long productCnt) {
            this.productCnt = productCnt;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(
                    new ListStateDescriptor<ProductViewCountPerWindow>(
                            "listState",
                            Types.POJO(ProductViewCountPerWindow.class)
                    )
            );
        }

        @Override
        public void processElement(ProductViewCountPerWindow value, Context ctx, Collector<String> out) throws Exception {

            // 每条访问记录添加到状态中维护起来
            listState.add(value);

            // 新建集合方便排序
            ArrayList<ProductViewCountPerWindow> list = new ArrayList<>();
            for (ProductViewCountPerWindow tmp : listState.get()) {
                list.add(tmp);
            }

            // 按照访问次数降序排序
            list.sort((t1, t2) -> (int) (t2.count - t1.count));

            // 当list集合中保存的元素个数 > productCnt时打印
            if (list.size() >= productCnt) {
                StringBuilder sb = new StringBuilder();
                sb.append("窗口结束时间：").append(new Timestamp(ctx.getCurrentKey())).append("\n");
                sb.append("=================================\n");
                for (int i = 0; i < productCnt; i++) {
                    ProductViewCountPerWindow pro = list.get(i);
                    sb
                            .append(i + 1)
                            .append("商品：").append(pro.productId)
                            .append("数量：").append(pro.count)
                            .append("\n");
                }
                sb.append("=================================\n");

                // 返回结果
                out.collect(sb.toString());
            }
        }
    }
}
