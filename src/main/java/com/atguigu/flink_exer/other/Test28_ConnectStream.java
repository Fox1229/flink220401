package com.atguigu.flink_exer.other;

import com.atguigu.flink.pojo.Event;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

public class Test28_ConnectStream {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> leftStream = env
                .addSource(new SourceFunction<Event>() {
                    @Override
                    public void run(SourceContext<Event> ctx) throws Exception {
                        ctx.collectWithTimestamp(new Event("key-1", "left", 1000L), 1000L);
                        ctx.collectWithTimestamp(new Event("key-2", "left", 2000L), 2000L);
                        // �ύˮλ��
                        ctx.emitWatermark(new Watermark(20000L));
                    }

                    @Override
                    public void cancel() {

                    }
                });

        DataStreamSource<Event> rightStream = env
                .addSource(new SourceFunction<Event>() {
                    @Override
                    public void run(SourceContext<Event> ctx) throws Exception {
                        ctx.collectWithTimestamp(new Event("key-1", "right", 3000L), 3000L);
                        ctx.collectWithTimestamp(new Event("key-3", "right", 7000L), 7000L);
                        // �ύˮλ��
                        ctx.emitWatermark(new Watermark(20000L));
                        ctx.collectWithTimestamp(new Event("key-2", "right", 30000L), 30000L);
                    }

                    @Override
                    public void cancel() {

                    }
                });

        leftStream
                .keyBy(r -> r.username)
                .connect(rightStream.keyBy(r -> r.username))
                .process(new Match())
                .print();

        env.execute();
    }

    public static class Match extends CoProcessFunction<Event, Event, String> {
        private ValueState<Event> leftState;
        private ValueState<Event> rightState;

        @Override
        public void open(Configuration parameters) throws Exception {
            leftState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Event>("leftState", Event.class)
            );

            rightState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Event>("rightState", Event.class)
            );
        }

        @Override
        public void processElement1(Event event, CoProcessFunction<Event, Event, String>.Context ctx, Collector<String> out) throws Exception {
            // �������ݵ���ж���������
            if (rightState.value() != null) {
                // ���������Ѿ�����
                out.collect(event.username + "���˳ɹ���right�ȵ�");
                // �������״̬
                rightState.clear();
            } else {
                // ����Ϊ�գ�ע�ᶨʱ��
                ctx.timerService().registerEventTimeTimer(event.ts + 5000);
                // ��������״̬
                leftState.update(event);
            }
        }

        @Override
        public void processElement2(Event event, CoProcessFunction<Event, Event, String>.Context ctx, Collector<String> out) throws Exception {
            // �������ݵ���ж���������
            if (leftState.value() != null) {
                // ���������Ѿ�����
                out.collect(event.username + "���˳ɹ���left�ȵ�");
                // �������״̬
                leftState.clear();
            } else {
                // ��������Ϊ�գ�ע�ᶨʱ��
                ctx.timerService().registerEventTimeTimer(event.ts + 5000);
                // ��������״̬
                rightState.update(event);
            }
        }

        @Override
        public void onTimer(long timestamp, CoProcessFunction<Event, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // ��ʱ��������˵���������ʧ��
            if (leftState.value() != null) {
                out.collect(leftState.value().username + "����ʧ�ܣ�rightû��");
                leftState.clear();
            }

            if (rightState.value() != null) {
                out.collect(rightState.value().username + "����ʧ�ܣ�leftû��");
                rightState.clear();
            }
        }
    }
}
