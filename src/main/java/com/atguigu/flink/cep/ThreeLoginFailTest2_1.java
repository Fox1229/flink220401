package com.atguigu.flink.cep;

import com.atguigu.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * 连续3次登录失败
 */
public class ThreeLoginFailTest2_1 {

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
                .process(
                        new KeyedProcessFunction<String, Event, String>() {
                            @Override
                            public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {

                            }
                        })
                .print();

        env.execute();
    }
}
