package com.atguigu.flink_exer.other;

import com.atguigu.flink.pojo.Event;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

// 实现两条流的内连接查询
public class Test27_ConnectStream {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> leftStream = env.fromElements(
                new Event("key-1", "left", 1000L),
                new Event("key-2", "left", 2000L),
                new Event("key-1", "left", 3000L),
                new Event("key-2", "left", 4000L)
        );

        DataStreamSource<Event> rightStream = env.fromElements(
                new Event("key-1", "right", 4000L),
                new Event("key-2", "right", 5000L),
                new Event("key-1", "right", 6000L),
                new Event("key-3", "right", 7000L)
        );

        leftStream
                .keyBy(r -> r.username)
                .connect(rightStream.keyBy(r -> r.username))
                .process(new InnerJoin())
                .print();

        env.execute();
    }

    public static class InnerJoin extends CoProcessFunction<Event, Event, String> {
        private ListState<Event> historyLeft;
        private ListState<Event> historyRight;

        @Override
        public void open(Configuration parameters) throws Exception {
            historyLeft = getRuntimeContext().getListState(
                    new ListStateDescriptor<Event>("historyLeft", Event.class)
            );

            historyRight = getRuntimeContext().getListState(
                    new ListStateDescriptor<Event>("historyRight", Event.class)
            );
        }

        @Override
        public void processElement1(Event event, CoProcessFunction<Event, Event, String>.Context ctx, Collector<String> out) throws Exception {
            // 保存数据
            historyLeft.add(event);
            // 遍历另一条流，进行关联
            for (Event tempEvent : historyRight.get()) {
                out.collect(event + " => " + tempEvent);
            }
        }

        @Override
        public void processElement2(Event event, CoProcessFunction<Event, Event, String>.Context ctx, Collector<String> out) throws Exception {
            // 保存数据
            historyRight.add(event);
            // 遍历另一条流，进行关联
            for (Event tempEvent : historyLeft.get()) {
                out.collect(tempEvent + " => " + event);
            }
        }
    }
}
