package com.atguigu.flink_exer.other;

import com.atguigu.flink.datastreamapi.transform.ClickSource;
import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.pojo.UserViewCountPerWindow;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class Test8_UserViewCountTop3 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner((bean, l) -> bean.ts))
                // 计算用户在每个窗口的访问次数
                .keyBy(bean -> bean.username)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .aggregate(new MyAcc(), new MyFun())
                // 计算同一窗口用户访问次数TopN
                .keyBy(bean -> bean.windowStopTime)
                .process(new MyProcessFun(3))
                .print();

        env.execute();
    }

    public static class MyProcessFun extends KeyedProcessFunction<Long, UserViewCountPerWindow, String> {
        private int uvCnt;
        private ListState<UserViewCountPerWindow> listState;

        public MyProcessFun(int uvCnt) {
            this.uvCnt = uvCnt;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 注册状态，保存窗口数据
            ListStateDescriptor<UserViewCountPerWindow> listStateDescriptor = new ListStateDescriptor<>("userViewListState", UserViewCountPerWindow.class);
            listState = getRuntimeContext().getListState(listStateDescriptor);
        }

        @Override
        public void processElement(UserViewCountPerWindow value, KeyedProcessFunction<Long, UserViewCountPerWindow, String>.Context ctx, Collector<String> out) throws Exception {
            // 数据到达，将数据保存到状态中
            listState.add(value);

            // 注册定时器
            ctx.timerService().registerEventTimeTimer(value.windowStopTime + 1);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, UserViewCountPerWindow, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发
            // 读取本窗口所有数据
            List<UserViewCountPerWindow> list = new ArrayList<>();
            for (UserViewCountPerWindow temp : listState.get()) {
                list.add(temp);
            }
            // 根据访问次数降序排序
            list.sort((u1, u2) -> (int) (u2.count - u1.count));

            // 输出TopN
            StringBuilder sb = new StringBuilder();
            sb.append("窗口结束时间").append(new Timestamp(timestamp)).append("\n").append("=======================\n");
            for (int i = 0; i < uvCnt; i++) {
                UserViewCountPerWindow userInfo = list.get(i);
                sb
                        .append(i + 1)
                        .append("-用户_").append(userInfo.username)
                        .append("-访问次数_").append(userInfo.count)
                        .append("\n");
            }
            sb.append("=======================\n");

            out.collect(String.valueOf(sb));
        }
    }

    public static class MyFun extends ProcessWindowFunction<Long, UserViewCountPerWindow, String, TimeWindow> {
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

    public static class MyAcc implements AggregateFunction<Event, Long, Long> {
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
