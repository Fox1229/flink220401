package com.atguigu.flink_exer.other;

import com.atguigu.flink.pojo.UVBean;
import com.atguigu.flink.pojo.UserBehavior;
import com.atguigu.flink.util.DateFormatUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 统计每小时UV数
 */
public class Test6_UV {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .readTextFile("D:\\developer\\idea workspace\\flink220401\\data\\UserBehavior.csv")
                .flatMap(new EtlFunction())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                        .withTimestampAssigner((bean, l) -> bean.ts))
                .keyBy(r -> r.userId)
                .process(new ProcessFunction())
                .keyBy(r -> 1)
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.hours(1L)))
                .reduce(new ReduceFunction<UVBean>() {
                            @Override
                            public UVBean reduce(UVBean value1, UVBean value2) throws Exception {
                                value1.uvCt = value1.uvCt + value2.uvCt;
                                return value1;
                            }
                        },
                        new WindowFunction<UVBean, UVBean, Integer, TimeWindow>() {
                            @Override
                            public void apply(Integer integer, TimeWindow window, Iterable<UVBean> input, Collector<UVBean> out) throws Exception {
                                UVBean bean = input.iterator().next();
                                bean.stt = DateFormatUtil.tsToDateTime(window.getStart());
                                bean.edt = DateFormatUtil.tsToDateTime(window.getEnd());
                                bean.curDate = DateFormatUtil.tsToDateForPartition(window.getStart());
                                out.collect(bean);
                            }
                        })
                .print();

        env.execute();
    }

    public static class ProcessFunction extends KeyedProcessFunction<String, UserBehavior, UVBean> {
        private ValueState<String> lastLoginDateState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<String> lastLoginDateStateDesc = new ValueStateDescriptor<>("lastLoginDateStateDesc", String.class);
            lastLoginDateStateDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.hours(1L)).build());
            lastLoginDateState = getRuntimeContext().getState(lastLoginDateStateDesc);
        }

        @Override
        public void processElement(UserBehavior userBehavior, KeyedProcessFunction<String, UserBehavior, UVBean>.Context ctx, Collector<UVBean> out) throws Exception {
            // 本次访问小时数
            String currDateHour = DateFormatUtil.tsToDateHour(userBehavior.ts);
            String lastLoginDate = lastLoginDateState.value();

            // 用户当天首次访问
            if (!currDateHour.equals(lastLoginDate)) {
                // 更新状态
                lastLoginDateState.update(currDateHour);

                out.collect(new UVBean(1L));
            }
        }
    }

    public static class EtlFunction implements FlatMapFunction<String, UserBehavior> {
        @Override
        public void flatMap(String value, Collector<UserBehavior> out) throws Exception {
            String[] array = value.split(",");
            if ("pv".equals(array[3])) {
                out.collect(
                        new UserBehavior(
                                array[0],
                                array[1],
                                array[2],
                                array[3],
                                Long.parseLong(array[4]) * 1000
                        )
                );
            }
        }
    }
}
