package com.atguigu.flink_exer.other;

import com.atguigu.flink.datastreamapi.transform.ClickSource;
import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.pojo.UserViewCountPerWindow;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 每秒钟输出最近一分钟的用户点击数
 */
public class Test5_Trigger {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner((bean, l) -> bean.ts))
                .keyBy(event -> event.username)
                .window(TumblingEventTimeWindows.of(Time.minutes(1L)))
                .trigger(new MyTrigger())
                .aggregate(new UserViewAcc(), new UserViewFun())
                .print();

        env.execute();
    }

    public static class MyTrigger extends Trigger<Event, TimeWindow> {
        private ValueState<Boolean> flag;

        /**
         * 每来一条数据调用一次，为窗口中的第一个数据的时间戳紧挨着的整数秒注册定时器
         * 作用范围：当前窗口
         */
        @Override
        public TriggerResult onElement(Event element, long timestamp, TimeWindow window, TriggerContext context) throws Exception {
            flag = context.getPartitionedState(new ValueStateDescriptor<>("flag", Boolean.class));

            // 属于窗口的第一条数据到达，注册下一秒定时器
            if (flag.value() == null) {
                // 1234 + 1000 - 1234 % 1000 = 2234 - 234 = 2000
                long nextSec = element.ts + 1000 - element.ts % 1000;
                context.registerEventTimeTimer(nextSec);

                // 更新状态
                flag.update(true);
            }

            return TriggerResult.CONTINUE;
        }

        /**
         * 处理时间
         */
        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext context) throws Exception {
            return null;
        }

        /**
         * 事件时间
         * 到达trigger的水位线超过参数`time`时，触发调用
         */
        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext context) throws Exception {
            // 定时器在窗口结束前触发
            if (time < window.getEnd()) {
                // 如果下一个整数秒窗口没有结束
                // 注册下一个整数秒的定时器，继续触发onEventTime的执行
                if (time + 1000 < window.getEnd()) {
                    context.registerEventTimeTimer(time + 1000);
                }

                // 触发aggregate算子执行，不销毁窗口
                return TriggerResult.FIRE;
            }

            return TriggerResult.CONTINUE;
        }

        /**
         * 窗口闭合时调用
         */
        @Override
        public void clear(TimeWindow window, TriggerContext context) throws Exception {
            flag.clear();
        }
    }

    public static class UserViewFun extends ProcessWindowFunction<Long, UserViewCountPerWindow, String, TimeWindow> {
        @Override
        public void process(String username, ProcessWindowFunction<Long, UserViewCountPerWindow, String, TimeWindow>.Context context, Iterable<Long> elements, Collector<UserViewCountPerWindow> out) throws Exception {
            out.collect(
                    new UserViewCountPerWindow(
                            username,
                            elements.iterator().next(),
                            context.window().getStart(),
                            context.window().getEnd()
                    )
            );
        }
    }

    public static class UserViewAcc implements AggregateFunction<Event, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event value, Long accumulator) {
            return accumulator + 1;
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
