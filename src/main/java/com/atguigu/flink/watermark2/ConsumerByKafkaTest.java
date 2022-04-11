package com.atguigu.flink.watermark2;

import com.atguigu.flink.pojo.ProductViewCountPerWindow;
import com.atguigu.flink.pojo.UserBehavior;
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
 * 从kafka消费数据
 *
 * 从文件消费数据，默认会有+MAX刷新水位线，但是从kafka消费数据，不会提供，因此会比从文件读取的数据少
 */
public class ConsumerByKafkaTest {

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
                        WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
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
                .window(TumblingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(
                        new AggregateFunction<UserBehavior, Long, Long>() {
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
                        },
                        new ProcessWindowFunction<Long, ProductViewCountPerWindow, String, TimeWindow>() {
                            @Override
                            public void process(String s, Context context, Iterable<Long> elements, Collector<ProductViewCountPerWindow> out) throws Exception {
                                out.collect(
                                        new ProductViewCountPerWindow(s, elements.iterator().next(), context.window().getStart(), context.window().getEnd())
                                );
                            }
                        })
                .keyBy(r -> r.windowEndTime)
                .process(
                        new KeyedProcessFunction<Long, ProductViewCountPerWindow, String>() {

                            private ListState<ProductViewCountPerWindow> listState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                listState = getRuntimeContext().getListState(
                                        new ListStateDescriptor<ProductViewCountPerWindow>("listState", Types.POJO(ProductViewCountPerWindow.class))
                                );
                            }

                            @Override
                            public void processElement(ProductViewCountPerWindow value, Context ctx, Collector<String> out) throws Exception {
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

                                // 拼接结果
                                StringBuilder sb = new StringBuilder();
                                sb.append("结束时间：").append(new Timestamp(timestamp - 1)).append("\n");
                                sb.append("===========================\n");
                                for (int i = 0; i < 3; i++) {
                                    ProductViewCountPerWindow tmp = list.get(i);
                                    sb
                                            .append(i + 1)
                                            .append(": 商品: ").append(tmp.productId)
                                            .append(", 访问次数: ").append(tmp.count)
                                            .append("\n");
                                }
                                sb.append("===========================\n");

                                out.collect(sb.toString());
                            }
                        })
                .print();

        env.execute();
    }
}
