package com.atguigu.flink.watermark2;

import com.atguigu.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 使用列表实现全外连接
 */
public class TwoStreamImplFullJoinTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> leftStream = env
                .fromElements(
                        new Event("key-1", "left", 1000L),
                        new Event("key-2", "left", 2000L),
                        new Event("key-1", "left", 3000L)
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

        SingleOutputStreamOperator<Event> rightStream = env
                .fromElements(
                        new Event("key-1", "right", 1000L),
                        new Event("key-2", "right", 2000L),
                        new Event("key-1", "right", 3000L)
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

        leftStream
                .keyBy(l -> l.username)
                .connect(rightStream.keyBy(r -> r.username))
                .process(
                        new CoProcessFunction<Event, Event, String>() {

                            private ListState<Event> leftState;
                            private ListState<Event> rightState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                leftState = getRuntimeContext().getListState(
                                        new ListStateDescriptor<Event>("leftState", Types.POJO(Event.class))
                                );

                                rightState = getRuntimeContext().getListState(
                                        new ListStateDescriptor<Event>("rightState", Types.POJO(Event.class))
                                );
                            }

                            @Override
                            public void processElement1(Event event, CoProcessFunction<Event, Event, String>.Context ctx, Collector<String> out) throws Exception {

                                // 遍历rightState进行join
                                for (Event right : rightState.get()) {
                                    out.collect(event + " => " + right);
                                }

                                // 添加元素到leftState
                                leftState.add(event);
                            }

                            @Override
                            public void processElement2(Event event, CoProcessFunction<Event, Event, String>.Context ctx, Collector<String> out) throws Exception {

                                for (Event left : leftState.get()) {
                                    out.collect(event + " => " + left);
                                }

                                rightState.add(event);
                            }
                        })
                .print();

        env.execute();
    }
}
