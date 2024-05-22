package com.atguigu.flink_exer.other;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import java.sql.Timestamp;

public class Test14_OnTimer {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .socketTextStream("hadoop102", 6666)
                .keyBy(r -> 1)
                .process(new KeyedProcessFunction<Integer, String, String>() {
                    @Override
                    public void processElement(String value, KeyedProcessFunction<Integer, String, String>.Context context, Collector<String> out) throws Exception {
                        // ��ǰ����ʱ��
                        long currTs = context.timerService().currentProcessingTime();
                        // 10s֮���ʱ��
                        long tenAfterTs = currTs + 10 * 1000;
                        // ע��10s֮��Ķ�ʱ��
                        context.timerService().registerProcessingTimeTimer(tenAfterTs);

                        out.collect(value + "�����ǰ����ʱ��Ϊ" + new Timestamp(currTs) +
                                "��ע���˻���ʱ��Ϊ" + new Timestamp(tenAfterTs) + "�Ķ�ʱ��");
                    }

                    public void onTimer(long timestamp, KeyedProcessFunction<Integer, String, String>.OnTimerContext context, Collector<String> out) throws Exception {
                        // ��ʱ������
                        out.collect("��ʱ������" + new Timestamp(timestamp) +
                                "��������ǰ����ʱ��Ϊ" + new Timestamp(context.timerService().currentProcessingTime()));
                    }
                })
                .print();

        env.execute();
    }
}
