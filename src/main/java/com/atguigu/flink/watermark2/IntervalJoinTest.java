package com.atguigu.flink.watermark2;

import com.atguigu.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 基于时间的间隔
 * [过去1s, 未来5s]
 */
public class IntervalJoinTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> leftStream = env
                .fromElements(
                        new Event("key-1", "left", 10 * 1000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.ts;
                                    }
                                })
                );

        SingleOutputStreamOperator<Event> rightStream = env
                .fromElements(
                        new Event("key-1", "right", 2000L),
                        new Event("key-1", "right", 9500L),
                        new Event("key-1", "right", 13 * 1000L),
                        new Event("key-1", "right", 14 * 1000L),
                        new Event("key-1", "right", 17 * 1000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.ts;
                                    }
                                })
                );

        leftStream
                .keyBy(r -> r.username)
                .intervalJoin(rightStream.keyBy(r -> r.username))
                .between(Time.seconds(-1), Time.seconds(5))
                .process(
                        new ProcessJoinFunction<Event, Event, String>() {
                            @Override
                            public void processElement(Event left, Event right, ProcessJoinFunction<Event, Event, String>.Context ctx, Collector<String> out) throws Exception {

                                out.collect(left + " => " + right);
                            }
                        })
                .print();

        env.execute();
    }
}
