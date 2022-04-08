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

/**
 * 使用KeyedProcessFunction实现ProcessWindowFunction
 */
public class KeyedProcessFunctionImplProcessWindowFunTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .keyBy(r -> r.username)
                .process(new MyTumblingTimeWindow(5000L))
                .print();

        env.execute();
    }

    public static class MyTumblingTimeWindow extends KeyedProcessFunction<String, Event, UserViewCountPerWindow> {

        private Long windowSize;

        public MyTumblingTimeWindow(Long windowSize) {
            this.windowSize = windowSize;
        }

        // 每个用户维护一个MapState
        // key: 窗口开始时间
        // values: 窗口中所有元素组成的ArrayList
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

            // 计算数据到达时间所属的窗口
            long currentProcessingTime = ctx.timerService().currentProcessingTime();
            long windowStartTime = currentProcessingTime - currentProcessingTime % windowSize;

            // 判断该窗口开始时间是否存在
            if (!mapState.contains(windowStartTime)) {
                // 不存在，说明来的是该窗口的第一条数据
                ArrayList<Event> events = new ArrayList<>();
                events.add(event);
                mapState.put(windowStartTime, events);
            } else {
                // 存在，说明窗口已存在数据。获取该窗口开始时间对应的集合，并将数据添加到集合中
                mapState.get(windowStartTime).add(event);
            }

            // 在窗口事件-1毫秒处注册一个定时器
            ctx.timerService().registerProcessingTimeTimer(windowStartTime + windowSize - 1L);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, UserViewCountPerWindow>.OnTimerContext ctx, Collector<UserViewCountPerWindow> out) throws Exception {

            String username = ctx.getCurrentKey();
            long windowStartTime = timestamp + 1 - windowSize;
            long windowStopTime = windowStartTime + windowSize;
            Long count = (long) mapState.get(windowStartTime).size();

            out.collect(
                    new UserViewCountPerWindow(
                            username,
                            count,
                            windowStartTime,
                            windowStopTime
                    )
            );
        }
    }
}
