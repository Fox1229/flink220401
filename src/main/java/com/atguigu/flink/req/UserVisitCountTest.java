package com.atguigu.flink.req;

import com.atguigu.flink.pojo.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.HashSet;

/**
 * �������ÿ�
 * ʹ��HashSetά��
 * ���⣺
 *      �������еĶ����ÿ͹��࣬�ұ�����ֶνϳ����磺url��ʱ����Ҫ���ڴ��б������������
 *             1������ 0.1K��1���û� => 10G
 *      �����������ܶ�ʱ��ÿ�λ�����Ҫռ��10G�ڴ棬�е�����
 */
public class UserVisitCountTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .readTextFile("D:\\developer\\idea workspace\\flink220401\\data\\UserBehavior.csv")
                .map(
                        new MapFunction<String, UserBehavior>() {
                            @Override
                            public UserBehavior map(String value) throws Exception {
                                String[] fields = value.split(",");
                                return new UserBehavior(fields[0], fields[1], fields[2], fields[3], Long.parseLong(fields[4]) * 1000L);
                            }
                        })
                .filter(r -> "pv".equals(r.type))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserBehavior>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                                    @Override
                                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                        return element.ts;
                                    }
                                })
                )
                .keyBy(r -> 1)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(
                        new AggregateFunction<UserBehavior, HashSet<String>, Long>() {
                            @Override
                            public HashSet<String> createAccumulator() {
                                return new HashSet<>();
                            }

                            @Override
                            public HashSet<String> add(UserBehavior value, HashSet<String> accumulator) {
                                // ȥ��
                                accumulator.add(value.userId);
                                return accumulator;
                            }

                            @Override
                            public Long getResult(HashSet<String> accumulator) {
                                return (long) accumulator.size();
                            }

                            @Override
                            public HashSet<String> merge(HashSet<String> a, HashSet<String> b) {
                                return null;
                            }
                        },
                        new ProcessWindowFunction<Long, String, Integer, TimeWindow>() {
                            @Override
                            public void process(Integer integer, ProcessWindowFunction<Long, String, Integer, TimeWindow>.Context context, Iterable<Long> elements, Collector<String> out) throws Exception {

                                out.collect("����" + new Timestamp(context.window().getStart()) + "~"
                                        + new Timestamp(context.window().getEnd()) + "������"
                                        + elements.iterator().next() + "�������ÿ͡�");
                            }
                        }
                )
                .print();

        env.execute();
    }
}