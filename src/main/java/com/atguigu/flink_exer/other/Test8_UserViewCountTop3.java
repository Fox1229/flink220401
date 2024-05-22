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
                // �����û���ÿ�����ڵķ��ʴ���
                .keyBy(bean -> bean.username)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .aggregate(new MyAcc(), new MyFun())
                // ����ͬһ�����û����ʴ���TopN
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
            // ע��״̬�����洰������
            ListStateDescriptor<UserViewCountPerWindow> listStateDescriptor = new ListStateDescriptor<>("userViewListState", UserViewCountPerWindow.class);
            listState = getRuntimeContext().getListState(listStateDescriptor);
        }

        @Override
        public void processElement(UserViewCountPerWindow value, KeyedProcessFunction<Long, UserViewCountPerWindow, String>.Context ctx, Collector<String> out) throws Exception {
            // ���ݵ�������ݱ��浽״̬��
            listState.add(value);

            // ע�ᶨʱ��
            ctx.timerService().registerEventTimeTimer(value.windowStopTime + 1);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, UserViewCountPerWindow, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // ��ʱ������
            // ��ȡ��������������
            List<UserViewCountPerWindow> list = new ArrayList<>();
            for (UserViewCountPerWindow temp : listState.get()) {
                list.add(temp);
            }
            // ���ݷ��ʴ�����������
            list.sort((u1, u2) -> (int) (u2.count - u1.count));

            // ���TopN
            StringBuilder sb = new StringBuilder();
            sb.append("���ڽ���ʱ��").append(new Timestamp(timestamp)).append("\n").append("=======================\n");
            for (int i = 0; i < uvCnt; i++) {
                UserViewCountPerWindow userInfo = list.get(i);
                sb
                        .append(i + 1)
                        .append("-�û�_").append(userInfo.username)
                        .append("-���ʴ���_").append(userInfo.count)
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
