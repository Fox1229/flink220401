package com.atguigu.flink_exer.other;

import com.atguigu.flink.datastreamapi.transform.ClickSource;
import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.pojo.UserViewCountPerWindow;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 使用KeyProcessFunction实现AggregateFunction + ProcessWindowFunction的功能
 * 每5s统计每个用户访问次数
 */
public class Test23_Window {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner((bean, l) -> bean.ts))
                .keyBy(event -> event.username)
                .process(new ProcessFunction(5000))
                .print();

        env.execute();
    }

    public static class ProcessFunction extends KeyedProcessFunction<String, Event, UserViewCountPerWindow> {
        private int windowSize;
        private MapState<Long, Long> mapState;

        public ProcessFunction(int windowSize) {
            this.windowSize = windowSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 使用MapState保存窗口信息
            // key: 窗口开始时间
            // value: 用户在该窗口访问次数
            MapStateDescriptor<Long, Long> mapStateDesc = new MapStateDescriptor<>("mapStateDesc", Types.LONG, Types.LONG);
            mapState = getRuntimeContext().getMapState(mapStateDesc);
        }

        @Override
        public void processElement(Event event, KeyedProcessFunction<String, Event, UserViewCountPerWindow>.Context ctx, Collector<UserViewCountPerWindow> out) throws Exception {
            // 数据到达，计算数据所属窗口
            Long ts = event.ts;
            // 窗口开始时间
            long windowStartTs = ts - ts % windowSize;

            // 属于窗口的第一条数据到达
            if (!mapState.contains(windowStartTs)) {
                mapState.put(windowStartTs, 1L);
            } else {
                // 窗口第二条及之后的数据到达
                mapState.put(windowStartTs, mapState.get(windowStartTs) + 1);
            }

            // 注册定时器
            long windowMaxTs = windowStartTs + windowSize - 1;
            ctx.timerService().registerEventTimeTimer(windowMaxTs);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, UserViewCountPerWindow>.OnTimerContext ctx, Collector<UserViewCountPerWindow> out) throws Exception {
            // 定时器触发
            // 计算窗口开始时间
            long windowStartTs = timestamp - windowSize + 1;
            out.collect(
                    new UserViewCountPerWindow(
                            ctx.getCurrentKey(),
                            mapState.get(windowStartTs),
                            windowStartTs,
                            timestamp + 1
                    )
            );

            // 删除窗口
            mapState.remove(windowStartTs);
        }
    }
}
