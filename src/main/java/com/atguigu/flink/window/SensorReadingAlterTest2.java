package com.atguigu.flink.window;

import com.atguigu.flink.pojo.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 1s内温度连续上升
 */
public class SensorReadingAlterTest2 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new SensorReadingSource())
                .keyBy(r -> r.sensorId)
                .process(
                        new KeyedProcessFunction<String, SensorReading, String>() {

                            private ValueState<Double> lastTemp;
                            private ValueState<Long> timer;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                lastTemp = getRuntimeContext().getState(
                                        new ValueStateDescriptor<Double>("lastTemp", Types.DOUBLE)
                                );

                                timer = getRuntimeContext().getState(
                                        new ValueStateDescriptor<Long>("timer", Types.LONG)
                                );
                            }

                            @Override
                            public void processElement(SensorReading sensorReading, Context ctx, Collector<String> out) throws Exception {

                                Double prevTemp = lastTemp.value();
                                Double currentTemp = sensorReading.temp;
                                lastTemp.update(currentTemp);

                                if (prevTemp != null) {
                                    if (currentTemp > prevTemp && timer.value() == null) {
                                        // 温度上升且没有警报器
                                        long oneSecondsLater = ctx.timerService().currentProcessingTime() + 1000L;
                                        ctx.timerService().registerProcessingTimeTimer(oneSecondsLater);
                                        timer.update(oneSecondsLater);
                                    } else if (prevTemp > currentTemp && timer.value() != null) {
                                        // 温度下降且有警报器，删除警报器
                                        ctx.timerService().deleteProcessingTimeTimer(timer.value());
                                        timer.clear();
                                    }
                                }

                                System.out.println(sensorReading);
                            }

                            @Override
                            public void onTimer(long timestamp, KeyedProcessFunction<String, SensorReading, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                                // 触发警报器
                                out.collect(ctx.getCurrentKey() + " 连续1s温度上升");
                                timer.clear();
                            }
                        }
                )
                .print();

        env.execute();
    }
}
