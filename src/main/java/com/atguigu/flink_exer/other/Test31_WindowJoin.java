package com.atguigu.flink_exer.other;

import com.atguigu.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Test31_WindowJoin {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> leftStream = env.fromElements(
                new Event("key-1", "left", 1000L),
                new Event("key-2", "left", 2000L),
                new Event("key-1", "left", 3000L),
                new Event("key-2", "left", 4000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                .withTimestampAssigner((event, l) -> event.ts));

        SingleOutputStreamOperator<Event> rightStream = env.fromElements(
                new Event("key-1", "right", 2000L),
                new Event("key-2", "right", 3000L),
                new Event("key-1", "right", 4000L),
                new Event("key-2", "right", 9000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                .withTimestampAssigner((event, l) -> event.ts));

        leftStream
                .join(rightStream)
                .where(l -> l.username)
                .equalTo(r -> r.username)
                .window(TumblingEventTimeWindows.of(Time.seconds(5L)))
                .apply(new JoinFunction<Event, Event, String>() {
                    @Override
                    public String join(Event first, Event second) throws Exception {
                        return first + " => " + second;
                    }
                })
                .print();

        env.execute();
    }
}
