package com.atguigu.flink.window;

import com.atguigu.flink.datastreamapi.transform.ClickSource;
import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.pojo.UserViewCountPerWindow;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class KeyedProcessFunctionImplementsProcessWindowFunAndAccTest1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .keyBy(r -> r.username)
                .process(new MyTumblingTimeWindowAndAcc(5000L))
                .print();

        env.execute();
    }

    public static class MyTumblingTimeWindowAndAcc extends KeyedProcessFunction<String, Event, UserViewCountPerWindow> {

        private Long windowSize;

        public MyTumblingTimeWindowAndAcc(Long windowSize) {
            this.windowSize = windowSize;
        }

        // <窗口开始时间, 累加器>
        private MapState<Long, Long> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            mapState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<Long, Long>("window-start-time", Types.LONG, Types.LONG)
            );
        }

        @Override
        public void processElement(Event value, KeyedProcessFunction<String, Event, UserViewCountPerWindow>.Context ctx, Collector<UserViewCountPerWindow> out) throws Exception {

            // 计算窗口开始时间
            long currentTs = ctx.timerService().currentProcessingTime();
            long windowStartTime = currentTs - currentTs % windowSize;

            // 判断窗口开始时间是否存在
            if(!mapState.contains(windowStartTime)) {
                // 不存在
                mapState.put(windowStartTime, 1L);
            } else {
                // 存在
                mapState.put(windowStartTime, mapState.get(windowStartTime) + 1L);
            }

            // 注册定时器时间
            ctx.timerService().registerProcessingTimeTimer(windowStartTime + windowSize - 1L);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, UserViewCountPerWindow>.OnTimerContext ctx, Collector<UserViewCountPerWindow> out) throws Exception {

            String username = ctx.getCurrentKey();
            // 根据定时器触发时间，计算窗口开始和结束时间
            Long windowStartTime = timestamp + 1 - windowSize;
            Long windowStopTime = windowStartTime + windowSize;
            // 获取累加器的值
            Long count = mapState.get(windowStartTime);

            out.collect(new UserViewCountPerWindow(username, count, windowStartTime, windowStopTime));
        }
    }
}
