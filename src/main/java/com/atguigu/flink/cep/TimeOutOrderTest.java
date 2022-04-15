package com.atguigu.flink.cep;

import com.atguigu.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;


/**
 * 过滤超时未支付的订单
 */
public class TimeOutOrderTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> orderStream = env
                .fromElements(
                        new Event("order-1", "create", 1000L),
                        new Event("order-2", "create", 2000L),
                        new Event("order-1", "pay", 3000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Event>() {
                                            @Override
                                            public long extractTimestamp(Event element, long recordTimestamp) {
                                                return element.ts;
                                            }
                                        }
                                )
                );

        // 定义模板
        Pattern<Event, Event> pattern = Pattern
                .<Event>begin("create-order")
                .where(
                        new SimpleCondition<Event>() {
                            @Override
                            public boolean filter(Event event) throws Exception {
                                return event.url.equals("create");
                            }
                        })
                .next("pay-order")
                .where(
                        new SimpleCondition<Event>() {
                            @Override
                            public boolean filter(Event event) throws Exception {
                                return event.url.equals("pay");
                            }
                        }
                )
                // 两个事件在5s内连续发生
                .within(Time.seconds(5));

        SingleOutputStreamOperator<String> result = CEP
                .pattern(orderStream.keyBy(r -> r.username), pattern)
                .flatSelect(
                        // 超时未支付的订单
                        new OutputTag<String>("timeout-order") {
                        },
                        // 发送超时信息的匿名类
                        new PatternFlatTimeoutFunction<Event, String>() {
                            @Override
                            public void timeout(Map<String, List<Event>> map, long timeoutTimestamp, Collector<String> out) throws Exception {
                                // map {
                                //   "create-order": [Event]
                                // }
                                Event event = map.get("create-order").get(0);
                                out.collect("订单" + event.username + "超时未支付！");
                            }
                        },
                        // 正常支付的订单
                        new PatternFlatSelectFunction<Event, String>() {
                            @Override
                            public void flatSelect(Map<String, List<Event>> map, Collector<String> out) throws Exception {
                                Event createOrder = map.get("create-order").get(0);
                                Event payOrder = map.get("pay-order").get(0);
                                out.collect("订单" + createOrder.username + "支付成功！"
                                        + "订单创建时间：" + createOrder.ts
                                        + "订单支付时间：" + payOrder.ts
                                );
                            }
                        }
                );

        result.getSideOutput(new OutputTag<String>("timeout-order"){}).print("测输出流");
        result.print("主流");

        env.execute();
    }
}
