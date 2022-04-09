package com.atguigu.flink.watermark1;

import com.atguigu.flink.pojo.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * 实时热门商品
 * 方式一
 *      缺点：
 *          将所有的数据发送到同一个流去处理
 *          保存窗口的所有数据
 */
public class HotProductByTimeTest {

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
                .windowAll(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .process(new ProductPvTopN(3L))
                .print();

        env.execute();
    }

    public static class ProductPvTopN extends ProcessAllWindowFunction<UserBehavior, String, TimeWindow> {

        private final Long productCnt;

        public ProductPvTopN(Long productCnt) {
            this.productCnt = productCnt;
        }

        @Override
        public void process(Context context, Iterable<UserBehavior> elements, Collector<String> out) throws Exception {

            HashMap<String, Long> map = new HashMap<>();
            // 判断集合中是否包含该商品，没有添加，有则更新数据
            for (UserBehavior product : elements) {
                if(!map.containsKey(product.productId)) {
                    map.put(product.productId, 1L);
                } else {
                    map.put(product.productId, map.get(product.productId) + 1L);
                }
            }

            // 将map集合中的每个元素添加到list集合中排序
            ArrayList<Tuple2<String, Long>> list = new ArrayList<>();
            for (String s : map.keySet()) {
                list.add(Tuple2.of(s, map.get(s)));
            }

            // 按照访问次数降序排序
            list.sort((t1, t2) -> (int) (t2.f1 - t1.f1));

            // 取出list集合前三名
            StringBuilder sb = new StringBuilder();
            sb
                    .append("窗口").append(new Timestamp(context.window().getStart()))
                    .append("~").append(new Timestamp(context.window().getEnd()))
                    .append("\n");
            sb.append("====================================\n");
            for (int i = 0; i < productCnt; i++) {
                Tuple2<String, Long> tmp = list.get(i);
                sb
                        .append(i + 1)
                        .append(": 商品id: ").append(tmp.f0)
                        .append(", 点击次数: ").append(tmp.f1)
                        .append("\n");
            }
            sb.append("====================================\n");

            // 返回结果
            out.collect(sb.toString());
        }
    }
}
