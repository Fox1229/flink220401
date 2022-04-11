package com.atguigu.flink.watermark2;

import com.atguigu.flink.pojo.UserBehavior;
import com.atguigu.flink.pojo.UserViewCountPerWindow;
import org.apache.calcite.adapter.enumerable.AggAddContext;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Properties;

/**
 * 计算每5分钟近1个小时的访问次数top3
 */
public class ConsumerByKafkaTest1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        env
                .addSource(
                        new FlinkKafkaConsumer<String>(
                                "flink-userbehavior",
                                new SimpleStringSchema(),
                                properties
                        )
                )
                .setParallelism(4)
                .map(
                        new MapFunction<String, UserBehavior>() {
                            @Override
                            public UserBehavior map(String value) throws Exception {
                                String[] fields = value.split(",");
                                return new UserBehavior(fields[0], fields[1], fields[2], fields[3], Long.parseLong(fields[4]) * 1000L);
                            }
                        })
                .filter(r -> r.type.equals("pv"))
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
                .window(TumblingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                // 计算每个productId在每个窗口的访问次数
                .aggregate(new ProductAcc(), new ProductFun())
                // 将相同窗口结束时间商品路由到同一个并行子任务
                .keyBy(r -> r.windowStopTime)
                .process(new ProductClickTopN(3))
                .print();


        env.execute();
    }

    public static class ProductClickTopN extends KeyedProcessFunction<Long, UserViewCountPerWindow, String> {

        private final Integer productCnt;
        private ListState<UserViewCountPerWindow> listState;

        public ProductClickTopN(Integer productCnt) {
            this.productCnt = productCnt;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(
                    new ListStateDescriptor<UserViewCountPerWindow>("listState", Types.POJO(UserViewCountPerWindow.class))
            );
        }

        @Override
        public void processElement(UserViewCountPerWindow value, KeyedProcessFunction<Long, UserViewCountPerWindow, String>.Context ctx, Collector<String> out) throws Exception {
            listState.add(value);

            // 注册定时器当本窗口的数据都计算之后触发定时器执行
            ctx.timerService().registerEventTimeTimer(value.windowStopTime + 1L);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, UserViewCountPerWindow, String>.OnTimerContext ctx, Collector<String> out) throws Exception {

            // 获取listState保存的相同结束时间的数据
            ArrayList<UserViewCountPerWindow> list = new ArrayList<>();
            for (UserViewCountPerWindow tmp : listState.get()) {
                list.add(tmp);
            }

            // 降序排序
            list.sort((t1, t2) -> (int) (t2.count - t1.count));

            // 取出top3,拼接结果
            StringBuilder sb = new StringBuilder();
            sb.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n");
            sb.append("=====================================\n");
            for (int i = 0; i < productCnt; i++) {
                UserViewCountPerWindow tmp = list.get(i);
                sb
                        .append(i + 1)
                        .append(": 商品: ").append(tmp.username)
                        .append(", 访问次数: ").append(tmp.count)
                        .append("\n");
            }
            sb.append("=====================================\n");

            out.collect(sb.toString());
        }
    }

    public static class ProductFun extends ProcessWindowFunction<Long, UserViewCountPerWindow, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<Long> elements, Collector<UserViewCountPerWindow> out) throws Exception {
            out.collect(
                    new UserViewCountPerWindow(s, elements.iterator().next(), context.window().getStart(), context.window().getEnd())
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
