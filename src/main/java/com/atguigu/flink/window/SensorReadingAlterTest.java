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
 * 1s内温度连续上升警报
 */
public class SensorReadingAlterTest {

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

        // 保存上一次温度值
        private ValueState<Double> prevTemp;
        // 保存报警定时器的时间戳
        private ValueState<Long> timerTs;

        // 初始化状态
        @Override
        public void open(Configuration parameters) throws Exception {
            prevTemp = getRuntimeContext().getState(
                    new ValueStateDescriptor<Double>("prevTemp", Types.DOUBLE)
            );
            timerTs = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("timerTs", Types.LONG)
            );
        }

        // 1 2 3 4 5 2 7 8
        @Override
        public void processElement(SensorReading sensorReading, KeyedProcessFunction<String, SensorReading, String>.Context ctx, Collector<String> out) throws Exception {

            // 获取上一次温度
            Double lastTemp = prevTemp.value();
            // 获取当前温度，将当前温度保存为最新的温度
            Double nowTemp = sensorReading.temp;
            prevTemp.update(nowTemp);

            // 判断是否有上一次温度，如果是第一条数据则不做处理（只关心变化的数据）
            if(lastTemp != null) {
                if(nowTemp > lastTemp && timerTs.value() == null) {
                    // 温度上升且没有报警定时器，注册1s后的定时器
                    long oneSecondsLater = ctx.timerService().currentProcessingTime() + 1000L;
                    ctx.timerService().registerProcessingTimeTimer(oneSecondsLater);
                    // 将报警定时器的时间戳保存在timerTs中
                    timerTs.update(oneSecondsLater);
                } else if(lastTemp > nowTemp && timerTs.value() != null) {
                    // 温度下降，且存在定时器，删除报警定时器 => 手动将报警定时器从队列中删除
                    ctx.timerService().deleteProcessingTimeTimer(timerTs.value());
                    // 将timerTs清空
                    timerTs.clear();
                }
            }

            System.out.println(sensorReading);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, SensorReading, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // 打印警告信息
            out.collect("定时器" + ctx.getCurrentKey() + "连续1s温度上升！");
            // 将timerTs清空，方便注册新的报警定时器
            timerTs.clear();
        }
    }
}
