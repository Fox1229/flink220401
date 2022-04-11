package com.atguigu.flink.watermark2;

import com.atguigu.flink.pojo.Event;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

/**
 * 实时对账
 */
public class AppAndThreePayTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> leftStream = env
                .addSource(
                        new SourceFunction<Event>() {
                            @Override
                            public void run(SourceContext<Event> ctx) throws Exception {
                                ctx.collectWithTimestamp(
                                        new Event("key-1", "left", 1000L), 1000L
                                );

                                ctx.collectWithTimestamp(
                                        new Event("key-2", "left", 4000L), 4000L
                                );
                                ctx.emitWatermark(new Watermark(20000L));
                                Thread.sleep(2000L);
                            }

                            @Override
                            public void cancel() {

                            }
                        }
                );

        DataStreamSource<Event> rightStream = env
                .addSource(
                        new SourceFunction<Event>() {
                            @Override
                            public void run(SourceContext<Event> ctx) throws Exception {
                                ctx.collectWithTimestamp(
                                        new Event("key-1", "right", 4000L), 4000L
                                );

                                ctx.collectWithTimestamp(
                                        new Event("key-3", "right", 7000L), 7000L
                                );

                                ctx.emitWatermark(new Watermark(20000L));
                                Thread.sleep(2000L);
                                ctx.collectWithTimestamp(
                                        new Event("key-2", "right", 30000L), 30000L
                                );
                            }

                            @Override
                            public void cancel() {

                            }
                        }
                );

        leftStream
                .keyBy(r -> r.username)
                .connect(rightStream.keyBy(r -> r.username))
                .process(new Match())
                .print();

        env.execute();
    }

    public static class Match extends CoProcessFunction<Event, Event, String> {

        private ValueState<Event> leftState;
        private ValueState<Event> rightState;

        @Override
        public void open(Configuration parameters) throws Exception {
            leftState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Event>("leftState", Types.POJO(Event.class))
            );
            rightState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Event>("rightState", Types.POJO(Event.class))
            );
        }

        @Override
        public void processElement1(Event event, CoProcessFunction<Event, Event, String>.Context ctx, Collector<String> out) throws Exception {

            // 如果right事件先到达，那么right事件肯定被保存了下来
            // 所以rightState肯定不为空
            if(rightState.value() != null) {
                out.collect(event.username + "对账成功, right先到达");
                // 对账成功信息输出之后，清空rightState
                rightState.clear();
            } else {
                // leftState先到达，保存状态，并注册定时器
                leftState.update(event);
                ctx.timerService().registerEventTimeTimer(event.ts + 5000L);
            }
        }

        @Override
        public void processElement2(Event event, CoProcessFunction<Event, Event, String>.Context ctx, Collector<String> out) throws Exception {

            if(leftState.value() != null) {
                out.collect(event.username + "对账成功, left先到达");
                leftState.clear();
            } else {
                // right先到达
                rightState.update(event);
                ctx.timerService().registerEventTimeTimer(event.ts + 5000L);
            }
        }

        @Override
        public void onTimer(long timestamp, CoProcessFunction<Event, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {

            // 判断定时器中是否存在数据，若不存在则表示对账成功
            if(leftState.value() != null) {
                out.collect(leftState.value().username + "对账失败");
                leftState.clear();
            }

            if(rightState.value() != null) {
                out.collect(rightState.value().username + "对账失败");
                rightState.clear();
            }
        }
    }
}
