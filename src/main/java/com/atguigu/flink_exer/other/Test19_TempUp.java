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
 * 连续1s温度上升
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
                        // 初始化状态
                        // 保存设备上一次温度
                        ValueStateDescriptor<Double> lastTempStateDesc = new ValueStateDescriptor<>("lastTempStateDesc", Double.class);
                        lastTempState = getRuntimeContext().getState(lastTempStateDesc);

                        // 保存设备定时器时间
                        ValueStateDescriptor<Long> timerTsStateDesc = new ValueStateDescriptor<>("timerTsStateDesc", Long.class);
                        timerTsState = getRuntimeContext().getState(timerTsStateDesc);
                    }

                    @Override
                    public void processElement(SensorReading sensorReading, KeyedProcessFunction<String, SensorReading, String>.Context ctx, Collector<String> out) throws Exception {
                        // 获取设备上一次温度
                        Double lastTemp = lastTempState.value();
                        Double currTemp = sensorReading.temp;
                        // 更新状态
                        lastTempState.update(currTemp);

                        // 从第二条数据开始处理
                        if (lastTemp != null) {
                            // 温度上升且没有定时器
                            // 1s内温度多次上升，只在第一次温度上升时注册定时器
                            if (currTemp > lastTemp && timerTsState.value() == null) {
                                // 注册1s后的定时器
                                long ts = ctx.timerService().currentProcessingTime() + 1000;
                                ctx.timerService().registerProcessingTimeTimer(ts);
                                // 保存定时器时间
                                timerTsState.update(ts);
                            }
                            // 温度下降且有定时器
                            else if (lastTemp > currTemp && timerTsState.value() != null) {
                                // 清除定时器
                                ctx.timerService().deleteProcessingTimeTimer(timerTsState.value());
                                // 清除状态中保存的定时器时间，方便注册下一个定时器
                                timerTsState.clear();
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, SensorReading, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        // 定时器触发
                        out.collect(ctx.getCurrentKey() + "连续1s温度上升");

                        // 清除状态中保存的定时器时间，方便注册下一个定时器
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
