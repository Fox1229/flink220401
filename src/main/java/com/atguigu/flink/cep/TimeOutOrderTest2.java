package com.atguigu.flink.cep;

import com.atguigu.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;


/**
 * 过滤超时未支付的订单
 */
public class TimeOutOrderTest2 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> orderStream = env
                .fromElements(
                        new Event("order-1", "create", 1000L),
                        new Event("order-2", "pay", 2000L),
                        new Event("order-1", "pay", 3000L),
                        new Event("order-2", "create", 4000L)
//                        new Event("order-2", "pay", 10000L)
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

        SingleOutputStreamOperator<String> result = orderStream
                .keyBy(r -> r.username)
                .process(
                        new KeyedProcessFunction<String, Event, String>() {

                            private ValueState<Event> state;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                state = getRuntimeContext().getState(
                                        new ValueStateDescriptor<Event>("state", Types.POJO(Event.class))
                                );
                            }

                            @Override
                            public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {

                                if (event.url.equals("create")) {
                                    // 如果是创建订单的操作
                                    state.update(event);
                                    // 注册5s后的定时器
                                    ctx.timerService().registerEventTimeTimer(event.ts + 5000L);
                                } else if (event.url.equals("pay")) {
                                    if (state.value() != null && event.ts - state.value().ts <= 5000L) {
                                        out.collect("订单" + event.username + "正常支付，订单创建的时间是："
                                                + state.value().ts + "；订单支付时间是：" + event.ts);
                                        state.clear();
                                    }
                                }
                            }

                            @Override
                            public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {

                                if (state.value() != null && state.value().url.equals("create")) {
                                    ctx.output(new OutputTag<String>("timeout-order") {
                                               },
                                            "订单" + state.value().username + " 5s内未支付");
                                }
                            }
                        });

        result.getSideOutput(new OutputTag<String>("timeout-order") {
        }).print("测输出流");
        result.print("主流");

        env.execute();
    }
}
