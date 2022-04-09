package com.atguigu.flink.watermark1;

import com.atguigu.flink.pojo.Event;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * select * from A join B on A.id = B.id
 */
public class InnerJoinTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> leftStream = env
                .fromElements(
                        new Event("key-1", "left", 1000L),
                        new Event("key-2", "left", 2000L),
                        new Event("key-1", "left", 3000L),
                        new Event("key-2", "left", 4000L)
                );

        DataStreamSource<Event> rightStream = env
                .fromElements(
                        new Event("key-1", "right", 6000L),
                        new Event("key-2", "right", 7000L),
                        new Event("key-1", "right", 8000L),
                        new Event("key-2", "right", 9000L)
                );

        leftStream
                .keyBy(r -> r.username)
                .connect(rightStream.keyBy(r -> r.username))
                .process(new InnerJoin())
                .print();

        env.execute();
    }

    public static class InnerJoin extends CoProcessFunction<Event, Event, String> {

        private ListState<Event> history1;
        private ListState<Event> history2;

        @Override
        public void open(Configuration parameters) throws Exception {
            history1 = getRuntimeContext().getListState(
                    new ListStateDescriptor<Event>("history1", Types.POJO(Event.class))
            );
            history2 = getRuntimeContext().getListState(
                    new ListStateDescriptor<Event>("history2", Types.POJO(Event.class))
            );
        }

        @Override
        public void processElement1(Event event, CoProcessFunction<Event, Event, String>.Context ctx, Collector<String> out) throws Exception {

            history1.add(event);

            for (Event tmp : history2.get()) {
                out.collect(event + " => " + tmp);
            }
        }

        @Override
        public void processElement2(Event event, CoProcessFunction<Event, Event, String>.Context ctx, Collector<String> out) throws Exception {

            history2.add(event);

            for (Event tmp : history1.get()) {
                out.collect(tmp + " => " + event);
            }
        }
    }
}
