package com.atguigu.flink.lowapi;

import com.atguigu.flink.datastreamapi.transform.ClickSource;
import com.atguigu.flink.pojo.SensorReading;
import org.apache.commons.lang3.Validate;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 1s内温度连续上升警告
 */
public class ProcessWindowsFunTest1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new SensorReadingSource())
                .keyBy(r -> r.sensorId)
                .process(new TempAlter())
                .print();

        env.execute();
    }

    public static class TempAlter extends KeyedProcessFunction<String, SensorReading, String> {

        // 记录上一次温度值
        private ValueState<Double> prevTemp;
        // 记录警报器的时间戳
        private ValueState<Long> timerTs;

        @Override
        public void open(Configuration parameters) throws Exception {
            prevTemp = getRuntimeContext().getState(
                    new ValueStateDescriptor<Double>("prevTemp", Types.DOUBLE)
            );
            timerTs = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("timerTs", Types.LONG)
            );
        }

        @Override
        public void processElement(SensorReading sensorReading, KeyedProcessFunction<String, SensorReading, String>.Context ctx, Collector<String> out) throws Exception {

            // 获取上一次温度值
            Double lastTemp = this.prevTemp.value();
            // 获取当前温度并更新到lastTemp
            Double nowTemp = sensorReading.temp;
            prevTemp.update(nowTemp);

            // 判断是否有上一次温度，若没有，不作处理
            if (lastTemp != null) {
                if (nowTemp > lastTemp && timerTs.value() == null) {
                    // 温度上升，且没有警报器，创建1s后的警报器
                    long oneSecondsLater = ctx.timerService().currentProcessingTime() + 1000L;
                    ctx.timerService().registerProcessingTimeTimer(oneSecondsLater);
                    // 更新timerTs时间戳
                    timerTs.update(oneSecondsLater);
                } else if(lastTemp > nowTemp && timerTs.value() != null) {
                    // 温度下降，且定时器存在，删除警报器
                    ctx.timerService().deleteProcessingTimeTimer(timerTs.value());
                    // 清空警报器
                    timerTs.clear();
                }
            }

            System.out.println(sensorReading);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, SensorReading, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect("定时器 " + ctx.getCurrentKey() + " 连续1s温度上升警告！");
            // 清空警报器
            timerTs.clear();
        }
    }
}
