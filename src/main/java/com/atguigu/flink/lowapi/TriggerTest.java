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
public class TriggerTest {

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
                                        })
                )
                .keyBy(r -> r.username)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .trigger(new MyTrigger())
                .aggregate(new ClickAcc(), new ClickFun())
                .print();

        env.execute();
    }

    public static class MyTrigger extends Trigger<Event, TimeWindow> {

        ValueState<Boolean> flag = null;

        /**
         * 每来一条数据调用一次
         * 为窗口中的第一个数据的时间戳紧挨着的整数秒注册定时器
         * 当前窗口独有的状态变量
         */
        @Override
        public TriggerResult onElement(Event element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {

            // 标志位
            // 单例
            flag = ctx.getPartitionedState(new ValueStateDescriptor<Boolean>("flag", Types.BOOLEAN));

            // 如果flag为空，说明来的是第一条数据
            if (flag.value() == null) {
                // 将标志位置为true，那么第二条以及第二条之后的数据不会进入到if语句中
                flag.update(true);
                // 计算第一条数据的整数秒
                long nextSecond = element.ts + 1000L - element.ts % 1000L;
                // 注册定时器是onEventTime
                ctx.registerEventTimeTimer(nextSecond);
            }

            // 当onElement调用完，不对窗口做任何事情
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return null;
        }

        /**
         * 到达trigger的水位线超过参数`time`时，触发调用
         */
        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {

            if (time < window.getEnd()) {
                // 注册接下来的整数秒的定时器
                // 注册的还是onEventTime
                if (time + 1000 < window.getEnd()) {
                    ctx.registerEventTimeTimer(time + 1000L);
                }

                // 触发后面的aggregate算子，执行窗口聚合计算
                // 但不销毁窗口
                return TriggerResult.FIRE;
            }

            return TriggerResult.CONTINUE;
        }

        /**
         * 窗口闭合时调用
         */
        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            flag.clear();
        }
    }

    public static class ClickFun extends ProcessWindowFunction<Long, UserViewCountPerWindow, String, TimeWindow> {
        @Override
        public void process(String s, ProcessWindowFunction<Long, UserViewCountPerWindow, String, TimeWindow>.Context context, Iterable<Long> elements, Collector<UserViewCountPerWindow> out) throws Exception {
            out.collect(
                    new UserViewCountPerWindow(s, elements.iterator().next(), context.window().getStart(), context.window().getEnd())
            );
        }
    }

    public static class ClickAcc implements AggregateFunction<Event, Long, Long> {
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
