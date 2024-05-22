package com.atguigu.flink_exer.other;

import com.atguigu.flink.pojo.UserBehavior;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
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
import java.util.List;

/**
 * 缺点：
 *     1. 将数据写入同一个流，不能最大限度使用slot资源
 *     2. 使用Java HashMap保存所有数据
 */
public class Test1_HotProductByTime {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .readTextFile("D:\\developer\\idea workspace\\flink220401\\data\\UserBehavior.csv")
                .flatMap(new FlatMapFunction<String, UserBehavior>() {
                    @Override
                    public void flatMap(String value, Collector<UserBehavior> out) throws Exception {
                        String[] productInfo = value.split(",");
                        if ("pv".equals(productInfo[3])) {
                            out.collect(
                                    new UserBehavior(
                                            productInfo[0],
                                            productInfo[1],
                                            productInfo[2],
                                            productInfo[3],
                                            Long.parseLong(productInfo[4]) * 1000
                                    )
                            );
                        }
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                        .withTimestampAssigner((bean, l) -> bean.ts))
                // 将数据keyBy到同一个窗口中
                .windowAll(SlidingEventTimeWindows.of(Time.hours(1L), Time.minutes(5L)))
                .process(new ProductTopN(3))
                .print();

        env.execute();
    }

    public static class ProductTopN extends ProcessAllWindowFunction<UserBehavior, String, TimeWindow> {
        private int productCnt;

        public ProductTopN(int productCnt) {
            this.productCnt = productCnt;
        }

        @Override
        public void process(ProcessAllWindowFunction<UserBehavior, String, TimeWindow>.Context context, Iterable<UserBehavior> elements, Collector<String> out) throws Exception {
            // 保存商品访问次数
            HashMap<String, Long> map = new HashMap<>();
            for (UserBehavior userBehavior : elements) {
                String productId = userBehavior.productId;
                if (!map.containsKey(productId)) {
                    // 第一次保存，访问次数为1
                    map.put(productId, 1L);
                } else {
                    // 非第一次保存，累加
                    map.put(productId, map.get(productId) + 1L);
                }
            }

            // 将商品访问情况保存到List集合，方便排序
            List<Tuple2<String, Long>> list = new ArrayList<>();
            for (String key : map.keySet()) {
                list.add(Tuple2.of(key, map.get(key)));
            }
            // 按照浏览次数降序排序
            list.sort((p1, p2) -> (int) (p2.f1 - p1.f1));

            StringBuilder sb = new StringBuilder();
            sb
                    .append("窗口").append(new Timestamp(context.window().getStart()))
                    .append("~").append(new Timestamp(context.window().getEnd()))
                    .append("\n");
            sb.append("====================================\n");
            // 返回TopN
            for (int i = 0; i < productCnt; i++) {
                Tuple2<String, Long> tuple2 = list.get(i);
                sb
                        .append(i + 1)
                        .append(":商品_").append(tuple2.f0)
                        .append("-访问次数_").append(tuple2.f1)
                        .append("\n");
            }
            sb.append("====================================\n");

            out.collect(String.valueOf(sb));
        }
    }
}
