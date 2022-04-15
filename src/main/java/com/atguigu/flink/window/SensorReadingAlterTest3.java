package com.atguigu.flink.window;

import com.atguigu.flink.pojo.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class SensorReadingAlterTest3 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new SensorReadingSource())
                .keyBy(r -> r.sensorId)
                .process(
                        new KeyedProcessFunction<String, SensorReading, String>() {

                            // 记录上一次温度
                            private ValueState<Double> prevTemp;
                            private ValueState<Long> timer;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                prevTemp = getRuntimeContext().getState(
                                        new ValueStateDescriptor<Double>("prevTemp", Types.DOUBLE)
                                );

                                timer = getRuntimeContext().getState(
                                        new ValueStateDescriptor<Long>("timer", Types.LONG)
                                );
                            }

                            @Override
                            public void processElement(SensorReading in, KeyedProcessFunction<String, SensorReading, String>.Context ctx, Collector<String> out) throws Exception {

                                // 获取上一次温度
                                Double lastTemp = prevTemp.value();
                                // 更新上一次温度
                                Double currentTemp = in.temp;
                                prevTemp.update(currentTemp);

                                // 存在上一次温度
                                if (lastTemp != null) {
                                    if (currentTemp > lastTemp && timer.value() == null) {
                                        // 温度上升且没有警报器
                                        long oneSecondsLater = ctx.timerService().currentProcessingTime() + 1000L;
                                        ctx.timerService().registerProcessingTimeTimer(oneSecondsLater);
                                        timer.update(oneSecondsLater);
                                    } else if (lastTemp > currentTemp && timer.value() != null) {
                                        // 温度下降且存在警报器, 删除警报器
                                        ctx.timerService().deleteProcessingTimeTimer(timer.value());
                                        timer.clear();
                                    }
                                }

                                System.out.println(in);
                            }

                            @Override
                            public void onTimer(long timestamp, KeyedProcessFunction<String, SensorReading, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                                out.collect(ctx.getCurrentKey() + " 连续1s温度上升");
                                timer.clear();
                            }
                        })
                .print();

        env.execute();
    }
}
