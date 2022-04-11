package com.atguigu.flink.lowapi;

import com.atguigu.flink.datastreamapi.transform.ClickSource;
import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.pojo.UserViewCountPerWindow;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 窗口中第一条数据的时间戳接下来的每个整数秒都要输出窗口的统计结果
 */
public class TriggerTest1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
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
                )
                .keyBy(r -> r.username)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .trigger(new MyTrigger())
                .aggregate(new EventAcc(), new EventFun())
                .print();

        env.execute();
    }

    public static class MyTrigger extends Trigger<Event, TimeWindow> {

        ValueState<Boolean> flag = null;

        /**
         * 每条数据触发，为每个窗口第一个数据的时间戳紧挨着的整数秒注册定时器
         */
        @Override
        public TriggerResult onElement(Event element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {

            // 标记位
            flag = ctx.getPartitionedState(new ValueStateDescriptor<Boolean>("flag", Types.BOOLEAN));

            // 判断是否是第一条数据
            if (flag.value() == null) {
                // 更新标志位
                flag.update(true);

                // 注册下一秒的定时器, 用于触发onEventTime
                long nextSeconds = element.ts + 1000L - element.ts % 1000L;
                ctx.registerEventTimeTimer(nextSeconds);
            }

            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return null;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            // 判断当前时间是否 > 当前事件时间，如果是的执行
            if (window.getEnd() > time) {

                // 注册下一秒的定时器
                ctx.registerEventTimeTimer(time + 1000L);

                // 触发执行aggregate和processFun
                return TriggerResult.FIRE;
            }

            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            flag.clear();
        }
    }

    public static class EventFun extends ProcessWindowFunction<Long, UserViewCountPerWindow, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<Long> elements, Collector<UserViewCountPerWindow> out) throws Exception {
            out.collect(
                    new UserViewCountPerWindow(s, elements.iterator().next(), context.window().getStart(), context.window().getEnd())
            );
        }
    }

    public static class EventAcc implements AggregateFunction<Event, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event value, Long accumulator) {
            return accumulator + 1L;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }
}
