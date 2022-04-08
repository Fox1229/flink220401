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

import java.util.ArrayList;
import java.util.List;

public class KeyedProcessFunctionImplementsProcessWindowFunTest1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .keyBy(r -> r.username)
                .process(new MyProcessWindowFunction(5000L))
                .print();

        env.execute();
    }

    public static class MyProcessWindowFunction extends KeyedProcessFunction<String, Event, UserViewCountPerWindow> {

        private Long windowSize;

        public MyProcessWindowFunction(Long windowSize) {
            this.windowSize = windowSize;
        }

        // 使用mapState维护每个用户在每个窗口的访问事件
        // <窗口开始时间, 访问事件集合>
        private MapState<Long, List<Event>> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            mapState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<Long, List<Event>>(
                            "window-start-time",
                            Types.LONG,
                            Types.LIST(Types.POJO(Event.class))
                    )
            );
        }

        @Override
        public void processElement(Event event, KeyedProcessFunction<String, Event, UserViewCountPerWindow>.Context ctx, Collector<UserViewCountPerWindow> out) throws Exception {

            // 获取当前操作时间
            long currentTs = ctx.timerService().currentProcessingTime();
            // 计算窗口开始时间
            long windowStartTime = currentTs - currentTs % windowSize;

            // 判断mapState中是否包含该窗口的开始时间
            if(!mapState.contains(windowStartTime)) {
                // 没有该窗口开始时间，说明是第一条数据
                ArrayList<Event> events = new ArrayList<>();
                events.add(event);
                mapState.put(windowStartTime, events);
            } else {
                // 存在窗口事件，将数据添加到窗口时间对应的集合中
                mapState.get(windowStartTime).add(event);
            }

            // 注册窗口停止时间的定时器
            ctx.timerService().registerProcessingTimeTimer(windowStartTime + windowSize - 1L);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, UserViewCountPerWindow>.OnTimerContext ctx, Collector<UserViewCountPerWindow> out) throws Exception {

            String username = ctx.getCurrentKey();
            // 根据定时器触发时间计算窗口开始和结束时间
            Long windowStartTime = timestamp + 1 - windowSize;
            Long windowStopTime = windowStartTime + windowSize;
            long count = (long)mapState.get(windowStartTime).size();

            out.collect(new UserViewCountPerWindow(username, count, windowStartTime, windowStopTime));
        }
    }
}
