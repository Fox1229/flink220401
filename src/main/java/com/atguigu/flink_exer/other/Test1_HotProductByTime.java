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
 * ȱ�㣺
 *     1. ������д��ͬһ��������������޶�ʹ��slot��Դ
 *     2. ʹ��Java HashMap������������
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
                // ������keyBy��ͬһ��������
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
            // ������Ʒ���ʴ���
            HashMap<String, Long> map = new HashMap<>();
            for (UserBehavior userBehavior : elements) {
                String productId = userBehavior.productId;
                if (!map.containsKey(productId)) {
                    // ��һ�α��棬���ʴ���Ϊ1
                    map.put(productId, 1L);
                } else {
                    // �ǵ�һ�α��棬�ۼ�
                    map.put(productId, map.get(productId) + 1L);
                }
            }

            // ����Ʒ����������浽List���ϣ���������
            List<Tuple2<String, Long>> list = new ArrayList<>();
            for (String key : map.keySet()) {
                list.add(Tuple2.of(key, map.get(key)));
            }
            // �������������������
            list.sort((p1, p2) -> (int) (p2.f1 - p1.f1));

            StringBuilder sb = new StringBuilder();
            sb
                    .append("����").append(new Timestamp(context.window().getStart()))
                    .append("~").append(new Timestamp(context.window().getEnd()))
                    .append("\n");
            sb.append("====================================\n");
            // ����TopN
            for (int i = 0; i < productCnt; i++) {
                Tuple2<String, Long> tuple2 = list.get(i);
                sb
                        .append(i + 1)
                        .append(":��Ʒ_").append(tuple2.f0)
                        .append("-���ʴ���_").append(tuple2.f1)
                        .append("\n");
            }
            sb.append("====================================\n");

            out.collect(String.valueOf(sb));
        }
    }
}
