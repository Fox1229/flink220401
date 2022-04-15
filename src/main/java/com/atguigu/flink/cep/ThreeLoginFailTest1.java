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
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * 连续3次登录失败
 */
public class ThreeLoginFailTest1 {

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

        // 定义模板
        Pattern<Event, Event> pattern = Pattern
                .<Event>begin("login-fail")
                .where(
                        new SimpleCondition<Event>() {
                            @Override
                            public boolean filter(Event value) throws Exception {
                                return value.url.equals("fail");
                            }
                        })
                .times(3);

        CEP
                // 在流上面匹配符合模板的事件组
                // patternStream就是匹配出的事件组成的流
                .pattern(loginStream.keyBy(r -> r.username), pattern)
                // 将匹配出的事件从模板流提取出来
                .flatSelect(
                        new PatternFlatSelectFunction<Event, String>() {
                            @Override
                            public void flatSelect(Map<String, List<Event>> map, Collector<String> out) throws Exception {

                                // map {
                                //   "login-fail": [Event,Event,Event]
                                // }
                                Event first = map.get("login-fail").get(0);
                                Event two = map.get("login-fail").get(1);
                                Event three = map.get("login-fail").get(2);
                                out.collect("用户" + first.username + "连续3次登录失败，时间戳是："
                                        + first.ts + ", " + two.ts + ", " + three.ts);
                            }
                        }
                )
                .print();

        env.execute();
    }
}
