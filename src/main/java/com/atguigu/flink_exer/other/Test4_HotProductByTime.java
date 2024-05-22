package com.atguigu.flink_exer.other;

import com.atguigu.flink.pojo.ProductViewCountPerWindow;
import com.atguigu.flink.pojo.UserBehavior;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
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
import java.util.List;

/**
 * 每5分钟统计最近1小时商品访问Top3
 */
public class Test4_HotProductByTime {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .readTextFile("D:\\developer\\idea workspace\\flink220401\\data\\UserBehavior.csv")
                .flatMap(new EtlStream())
                // 设置水位线
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                        .withTimestampAssigner((bean, l) -> bean.ts))
                // 计算商品在每个窗口的访问次数
                .keyBy(bean -> bean.productId)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5L)))
                // 增量聚合商品访问次数
                .aggregate(new AccFunction(), new ProcessFunction())
                // 计算同一个窗口中所有商品访问次数，筛选TopN
                .keyBy(r -> r.windowEndTime)
                .process(new ProcessTopNFunction(3))
                .print();

        env.execute();
    }

    public static class ProcessTopNFunction extends KeyedProcessFunction<Long, ProductViewCountPerWindow, String> {
        private int n;
        private ListState<ProductViewCountPerWindow> listState;

        public ProcessTopNFunction(int n) {
            this.n = n;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ListStateDescriptor<ProductViewCountPerWindow> listStateDesc = new ListStateDescriptor<>("listStateDesc", ProductViewCountPerWindow.class);
            listState = getRuntimeContext().getListState(listStateDesc);
        }

        @Override
        public void processElement(ProductViewCountPerWindow bean, KeyedProcessFunction<Long, ProductViewCountPerWindow, String>.Context ctx, Collector<String> out) throws Exception {
            // 窗口数据到达，将数据保存到状态中
            listState.add(bean);

            // 注册定时器，当窗口所有数据到达，输出TopN
            ctx.timerService().registerEventTimeTimer(bean.windowEndTime + 1);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, ProductViewCountPerWindow, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，读取状态数据
            // 将数据保存到list中，方便排序
            List<ProductViewCountPerWindow> list = new ArrayList<>();
            for (ProductViewCountPerWindow tempPv : listState.get()) {
                list.add(tempPv);
            }
            // 按照商品访问次数，降序排序
            list.sort((p1, p2) -> (int) (p2.count - p1.count));

            // 输出TopN
            StringBuilder sb = new StringBuilder();
            sb.append("窗口结束时间").append(new Timestamp(timestamp)).append("\n=============================\n");
            for (int i = 0; i < n; i++) {
                ProductViewCountPerWindow pv = list.get(i);
                sb
                        .append(i + 1)
                        .append("-商品_").append(pv.productId)
                        .append("-访问次数_").append(pv.count)
                        .append("\n");
            }
            sb.append("=============================\n");

            out.collect(String.valueOf(sb));
        }
    }

    public static class ProcessFunction extends ProcessWindowFunction<Long, ProductViewCountPerWindow, String, TimeWindow> {
        @Override
        public void process(String productId, ProcessWindowFunction<Long, ProductViewCountPerWindow, String, TimeWindow>.Context context, Iterable<Long> elements, Collector<ProductViewCountPerWindow> out) throws Exception {
            out.collect(
                    new ProductViewCountPerWindow(
                            productId,
                            elements.iterator().next(),
                            context.window().getStart(),
                            context.window().getEnd()
                    )
            );
        }
    }

    public static class AccFunction implements AggregateFunction<UserBehavior, Long, Long> {
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

    public static class EtlStream implements FlatMapFunction<String, UserBehavior> {
        @Override
        public void flatMap(String value, Collector<UserBehavior> out) throws Exception {
            String[] array = value.split(",");
            if ("pv".equals(array[3])) {
                out.collect(
                        new UserBehavior(
                                array[0],
                                array[1],
                                array[2],
                                array[3],
                                Long.parseLong(array[4]) * 1000
                        )
                );
            }
        }
    }
}
