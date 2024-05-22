package com.atguigu.flink_exer.other;

import com.atguigu.flink.pojo.IntStatistic;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

public class Test15_ValueState {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new SourceFunction<Integer>() {
                    private boolean flag = true;

                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        while (flag) {
                            Random random = new Random();
                            ctx.collect(random.nextInt(1000));

                            Thread.sleep(100L);
                        }
                    }

                    @Override
                    public void cancel() {
                        flag = false;
                    }
                })
                .keyBy(r -> 1)
                .process(new MyIntStatistic())
                .print();

        env.execute();
    }

    public static class MyIntStatistic extends KeyedProcessFunction<Integer, Integer, String> {
        private ValueState<IntStatistic> valueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // ����״̬��������
            ValueStateDescriptor<IntStatistic> valueStateDesc = new ValueStateDescriptor<>("valueStateDesc", IntStatistic.class);
            valueState = getRuntimeContext().getState(valueStateDesc);
        }

        @Override
        public void processElement(Integer value, KeyedProcessFunction<Integer, Integer, String>.Context context, Collector<String> out) throws Exception {
            // ��һ�����ݵ��ֱ�Ӹ���״̬
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

            // д������
            IntStatistic currVs = valueState.value();
            out.collect(
                    "����" + value +
                            "�����Сֵ" + currVs.min +
                            "�����ֵ" + currVs.max +
                            "���ܺ�" + currVs.sum +
                            "��������" + currVs.count +
                            "��ƽ��ֵ" + currVs.avg
            );
        }
    }
}
