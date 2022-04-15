package com.atguigu.flink.cep;

import com.atguigu.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 连续3次登录失败
 */
public class ThreeLoginFailTest2 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> loginStream = env
                .fromElements(
                        new Event("tom", "fail", 1000L),
                        new Event("tom", "fail", 2000L),
                        new Event("Jerry", "success", 3000L),
                        new Event("tom", "fail", 4000L),
                        new Event("tom", "fail", 5000L)
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

        loginStream
                .keyBy(r -> r.username)
                .process(new StateMachine())
                .print();

        env.execute();
    }

    public static class StateMachine extends KeyedProcessFunction<String, Event, String> {

        // hashmap保存状态机
        // (当前状态，接受的事件类型) => 将要跳转到的状态
        private HashMap<Tuple2<String, String>, String> stateMachine = new HashMap<>();
        // 保存当前状态
        private ValueState<String> currentState;

        @Override
        public void open(Configuration parameters) throws Exception {
            stateMachine.put(Tuple2.of("INITIAL", "fail"), "S1");
            stateMachine.put(Tuple2.of("S1", "fail"), "S2");
            stateMachine.put(Tuple2.of("S2", "fail"), "FAIL");
            stateMachine.put(Tuple2.of("INITIAL", "success"), "SUCCESS");
            stateMachine.put(Tuple2.of("S1", "success"), "SUCCESS");
            stateMachine.put(Tuple2.of("S2", "success"), "SUCCESS");

            currentState = getRuntimeContext().getState(
                    new ValueStateDescriptor<String>("currentState", Types.STRING)
            );
        }

        @Override
        public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {

            if (currentState.value() == null) {
                currentState.update("INITIAL");
            }

            // 根据当前状态和接收的事件类型查找要跳转的状态
            String nextState = stateMachine.get(Tuple2.of(currentState.value(), event.url));
            if (nextState.equals("FAIL")) {
                out.collect(event.username + "连续三次登录失败");
                // 重置到S2状态
                currentState.update("S2");
            } else if (nextState.equals("SUCCESS")) {
                out.collect(event.username + "登录成功");
                // 跳转到初始状态
                currentState.update("INITIAL");
            } else {
                // 跳转到下一个状态
                currentState.update(nextState);
            }
        }
    }
}
