package com.atguigu.flink_exer.other;

import com.atguigu.flink.pojo.IntStatistic;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import java.util.Random;

/**
 * ʹ�ö�ʱ������
 */
public class Test16_ValueState {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new SourceFunction<Tuple2<Integer, Long>>() {
                    private boolean flag = true;

                    @Override
                    public void run(SourceContext<Tuple2<Integer, Long>> ctx) throws Exception {
                        while (flag) {
                            Random random = new Random();

                            ctx.collect(
                                    Tuple2.of(
                                            random.nextInt(1000),
                                            System.currentTimeMillis()
                                    )
                            );

                            Thread.sleep(100L);
                        }
                    }

                    @Override
                    public void cancel() {
                        flag = false;
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<Integer, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner((t, l) -> t.f1))
                .keyBy(r -> 1)
                .process(new MyIntStatistic())
                .print();

        env.execute();
    }

    public static class MyIntStatistic extends KeyedProcessFunction<Integer, Tuple2<Integer, Long>, IntStatistic> {
        private ValueState<IntStatistic> valueState;
        private ValueState<Boolean> flag;

        @Override
        public void open(Configuration parameters) throws Exception {
            // ����״̬��������
            ValueStateDescriptor<IntStatistic> valueStateDesc = new ValueStateDescriptor<>("valueStateDesc", IntStatistic.class);
            valueState = getRuntimeContext().getState(valueStateDesc);

            ValueStateDescriptor<Boolean> booleanDesc = new ValueStateDescriptor<>("flag", Boolean.class);
            flag = getRuntimeContext().getState(booleanDesc);
        }

        @Override
        public void processElement(Tuple2<Integer, Long> kv, KeyedProcessFunction<Integer, Tuple2<Integer, Long>, IntStatistic>.Context context, Collector<IntStatistic> out) throws Exception {
            // ��һ�����ݵ��ֱ�Ӹ���״̬
            Integer value = kv.f0;
            if (valueState.value() == null) {
                valueState.update(
                        new IntStatistic(
                                value,
                                value,
                                value,
                                1,
                                value
                        )
                );
            } else {
                // �ǵ�һ�����ݵ����ȡ״̬���뱾�����ݾۺϣ�������״̬
                IntStatistic dataInfo = valueState.value();
                valueState.update(
                        new IntStatistic(
                                Math.min(dataInfo.min, value),
                                Math.max(dataInfo.max, value),
                                dataInfo.sum + value,
                                dataInfo.count + 1,
                                (dataInfo.sum + value) / (dataInfo.count + 1)
                        )
                );
            }

            // ��ʱ��Ϊ�գ�ע��5s��Ķ�ʱ��
            if (flag.value() == null) {
                context.timerService().registerEventTimeTimer(kv.f1 + 5 * 1000);

                // ���±�־λ
                flag.update(true);
            }
        }

        // ʹ�ö�ʱ������
        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Integer, Tuple2<Integer, Long>, IntStatistic>.OnTimerContext ctx, Collector<IntStatistic> out) throws Exception {
            // ��ʱ������
            // д������
            out.collect(valueState.value());

            // ��ʱ�������������ʱ����־λ
            flag.clear();
        }
    }
}
