package com.atguigu.flink_exer.other;

import com.atguigu.flink.pojo.Event;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

public class Test28_ConnectStream {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> leftStream = env
                .addSource(new SourceFunction<Event>() {
                    @Override
                    public void run(SourceContext<Event> ctx) throws Exception {
                        ctx.collectWithTimestamp(new Event("key-1", "left", 1000L), 1000L);
                        ctx.collectWithTimestamp(new Event("key-2", "left", 2000L), 2000L);
                        // 提交水位线
                        ctx.emitWatermark(new Watermark(20000L));
                    }

                    @Override
                    public void cancel() {

                    }
                });

        DataStreamSource<Event> rightStream = env
                .addSource(new SourceFunction<Event>() {
                    @Override
                    public void run(SourceContext<Event> ctx) throws Exception {
                        ctx.collectWithTimestamp(new Event("key-1", "right", 3000L), 3000L);
                        ctx.collectWithTimestamp(new Event("key-3", "right", 7000L), 7000L);
                        // 提交水位线
                        ctx.emitWatermark(new Watermark(20000L));
                        ctx.collectWithTimestamp(new Event("key-2", "right", 30000L), 30000L);
                    }

                    @Override
                    public void cancel() {

                    }
                });

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
                    new ValueStateDescriptor<Event>("leftState", Event.class)
            );

            rightState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Event>("rightState", Event.class)
            );
        }

        @Override
        public void processElement1(Event event, CoProcessFunction<Event, Event, String>.Context ctx, Collector<String> out) throws Exception {
            // 左流数据到达，判断右流数据
            if (rightState.value() != null) {
                // 右流数据已经到达
                out.collect(event.username + "对账成功，right先到");
                // 清空右流状态
                rightState.clear();
            } else {
                // 右流为空，注册定时器
                ctx.timerService().registerEventTimeTimer(event.ts + 5000);
                // 更新左流状态
                leftState.update(event);
            }
        }

        @Override
        public void processElement2(Event event, CoProcessFunction<Event, Event, String>.Context ctx, Collector<String> out) throws Exception {
            // 右流数据到达，判断左流数据
            if (leftState.value() != null) {
                // 左流数据已经到达
                out.collect(event.username + "对账成功，left先到");
                // 清空左流状态
                leftState.clear();
            } else {
                // 左流数据为空，注册定时器
                ctx.timerService().registerEventTimeTimer(event.ts + 5000);
                // 更新右流状态
                rightState.update(event);
            }
        }

        @Override
        public void onTimer(long timestamp, CoProcessFunction<Event, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，说明任务对账失败
            if (leftState.value() != null) {
                out.collect(leftState.value().username + "对账失败，right没来");
                leftState.clear();
            }

            if (rightState.value() != null) {
                out.collect(rightState.value().username + "对账失败，left没来");
                rightState.clear();
            }
        }
    }
}
