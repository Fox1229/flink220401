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

public class KeyedProcessFunctionImplProcessWindowFunAndAccTest {

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

        // <窗口开始时间, 窗口中的累加器>
        private MapState<Long, Long> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            mapState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<Long, Long>(
                            "window-start-time",
                            Types.LONG,
                            Types.LONG
                    )
            );
        }

        @Override
        public void processElement(Event value, KeyedProcessFunction<String, Event, UserViewCountPerWindow>.Context ctx, Collector<UserViewCountPerWindow> out) throws Exception {

            // 计算窗口开始时间
            long currentTs = ctx.timerService().currentProcessingTime();
            long windowStartTime = currentTs - currentTs % windowSize;

            // 如果mapState中没有windowStartTime这个key，说明不存在这个窗口
            // 也就是说属于这个窗口的第一条数据到来，才会开窗口
            // 以下逻辑实现了AggregateFunction中的createAccumulator+add操作
            if (!mapState.contains(windowStartTime)) {
                // 第一条数据到达，累加器的值是1
                mapState.put(windowStartTime, 1L);
            } else {
                // 窗口中其他数据到达，累加器加1
                mapState.put(windowStartTime, mapState.get(windowStartTime) + 1L);
            }

            // 注册累加器窗口
            ctx.timerService().registerProcessingTimeTimer(windowStartTime + windowSize - 1);
        }

        /**
         * onTimer实现了getResult+process
         */
        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, UserViewCountPerWindow>.OnTimerContext ctx, Collector<UserViewCountPerWindow> out) throws Exception {

            // 根据触发时间计算开始和结束时间
            String username = ctx.getCurrentKey();
            Long windowStartTime = timestamp + 1 - windowSize;
            Long windowStopTime = windowStartTime + windowSize;
            // 获取窗口中累加器的值，类似getResult
            Long count = mapState.get(windowStartTime);

            out.collect(new UserViewCountPerWindow(username, count, windowStartTime, windowStopTime));
        }
    }
}
