package com.atguigu.flink_exer.other;

import com.atguigu.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class Test30_IntervalJoin {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> leftStream = env.fromElements(
                new Event("key-1", "left", 10 * 1000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                .withTimestampAssigner((event, l) -> event.ts));

        SingleOutputStreamOperator<Event> rightStream = env.fromElements(
                new Event("key-1", "right", 2 * 1000L),
                new Event("key-1", "right", 9 * 1000L),
                new Event("key-1", "right", 11 * 1000L),
                new Event("key-1", "right", 15 * 1000L),
                new Event("key-1", "right", 30 * 1000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                .withTimestampAssigner((event, l) -> event.ts));

        leftStream
                .keyBy(r -> r.username)
                .intervalJoin(rightStream.keyBy(r -> r.username))
                // ¹ýÈ¥3s, Î´À´5s
                .between(Time.seconds(-3L), Time.seconds(5L))
                .process(new IntervalJoinFunction())
                .print();

        env.execute();
    }

    public static class IntervalJoinFunction extends ProcessJoinFunction<Event, Event, String> {
        @Override
        public void processElement(Event left, Event right, ProcessJoinFunction<Event, Event, String>.Context ctx, Collector<String> out) throws Exception {
            out.collect(left + " => " + right);
        }
    }
}
