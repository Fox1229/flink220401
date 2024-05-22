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

import java.util.ArrayList;
import java.util.List;

/**
 * 使用KeyProcessFunction实现ProcessWindowFunction的功能
 * 每5s统计每个用户访问次数
 */
public class Test22_Window {

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
        private MapState<Long, List<Event>> mapState;

        public ProcessFunction(int windowSize) {
            this.windowSize = windowSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 使用MapState保存窗口信息
            // key: 窗口开始时间
            // value: ArrayList保存本窗口数据
            MapStateDescriptor<Long, List<Event>> mapStateDesc = new MapStateDescriptor<>("mapStateDesc", Types.LONG, Types.LIST(Types.POJO(Event.class)));
            mapState = getRuntimeContext().getMapState(mapStateDesc);
        }

        @Override
        public void processElement(Event event, KeyedProcessFunction<String, Event, UserViewCountPerWindow>.Context ctx, Collector<UserViewCountPerWindow> out) throws Exception {
            // 数据到达，计算数据所属窗口
            Long ts = event.ts;
            // 窗口开始时间
            long windowStartTs = ts - ts % windowSize;

            if (!mapState.contains(windowStartTs)) {
                // 如果窗口不存在（窗口第一条数据），保存窗口开始时间及数据
                ArrayList<Event> list = new ArrayList<>();
                list.add(event);
                mapState.put(windowStartTs, list);
            } else {
                // 如果窗口存在，新增本次数据
                mapState.get(windowStartTs).add(event);
            }

            // 窗口最大时间戳
            long windowMaxTs = windowStartTs + windowSize - 1;
            // 注册定时器
            ctx.timerService().registerEventTimeTimer(windowMaxTs);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, UserViewCountPerWindow>.OnTimerContext ctx, Collector<UserViewCountPerWindow> out) throws Exception {
            // 定时器触发
            // 还原窗口开始时间
            // timestamp: 窗口最大时间戳
            long windowStartTs = timestamp - windowSize + 1;
            // 获取窗口数据
            long count = mapState.get(windowStartTs).size();

            out.collect(
                    new UserViewCountPerWindow(
                            ctx.getCurrentKey(),
                            count,
                            windowStartTs,
                            timestamp + 1
                    )
            );

            // 销毁窗口
            mapState.remove(windowStartTs);
        }
    }
}
