package com.atguigu.flink_exer.other;

import com.atguigu.flink.pojo.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;


/**
 * ����1s�¶�����
 */
public class Test19_TempUp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new MySensor())
                .keyBy(bean -> bean.sensorId)
                .process(new KeyedProcessFunction<String, SensorReading, String>() {
                    private ValueState<Double> lastTempState;
                    private ValueState<Long> timerTsState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // ��ʼ��״̬
                        // �����豸��һ���¶�
                        ValueStateDescriptor<Double> lastTempStateDesc = new ValueStateDescriptor<>("lastTempStateDesc", Double.class);
                        lastTempState = getRuntimeContext().getState(lastTempStateDesc);

                        // �����豸��ʱ��ʱ��
                        ValueStateDescriptor<Long> timerTsStateDesc = new ValueStateDescriptor<>("timerTsStateDesc", Long.class);
                        timerTsState = getRuntimeContext().getState(timerTsStateDesc);
                    }

                    @Override
                    public void processElement(SensorReading sensorReading, KeyedProcessFunction<String, SensorReading, String>.Context ctx, Collector<String> out) throws Exception {
                        // ��ȡ�豸��һ���¶�
                        Double lastTemp = lastTempState.value();
                        Double currTemp = sensorReading.temp;
                        // ����״̬
                        lastTempState.update(currTemp);

                        // �ӵڶ������ݿ�ʼ����
                        if (lastTemp != null) {
                            // �¶�������û�ж�ʱ��
                            // 1s���¶ȶ��������ֻ�ڵ�һ���¶�����ʱע�ᶨʱ��
                            if (currTemp > lastTemp && timerTsState.value() == null) {
                                // ע��1s��Ķ�ʱ��
                                long ts = ctx.timerService().currentProcessingTime() + 1000;
                                ctx.timerService().registerProcessingTimeTimer(ts);
                                // ���涨ʱ��ʱ��
                                timerTsState.update(ts);
                            }
                            // �¶��½����ж�ʱ��
                            else if (lastTemp > currTemp && timerTsState.value() != null) {
                                // �����ʱ��
                                ctx.timerService().deleteProcessingTimeTimer(timerTsState.value());
                                // ���״̬�б���Ķ�ʱ��ʱ�䣬����ע����һ����ʱ��
                                timerTsState.clear();
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, SensorReading, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        // ��ʱ������
                        out.collect(ctx.getCurrentKey() + "����1s�¶�����");

                        // ���״̬�б���Ķ�ʱ��ʱ�䣬����ע����һ����ʱ��
                        timerTsState.clear();
                    }
                })
                .print();

        env.execute();
    }

    public static class MySensor implements SourceFunction<SensorReading> {
        private boolean flag = true;
        private Random random = new Random();

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            while (flag) {
                String sensorId = "sensor_" + (random.nextInt(4) + 1);
                double temp = random.nextGaussian();
                ctx.collect(new SensorReading(sensorId, temp));

                Thread.sleep(300L);
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }
}
