package com.atguigu.flink_exer.skew_data;

import com.atguigu.flink.datastreamapi.transform.ClickSource;
import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.pojo.UserViewCountPerWindow;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * keyBy之后的聚合出现数据倾斜
 */
public class Test33_SkewDataSum {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10013);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(2);
        // 禁用任务链
        env.disableOperatorChaining();

        env
                .addSource(new ClickSource())
                // 模拟倾斜数据
                .flatMap(new EtlStream())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner((event, l) -> event.ts))
                .map(new MyMapFun())
                .keyBy(r -> r.username)
                .reduce(new MyReduceFun())
                .print();

        env.execute();
    }

    public static class MyReduceFun implements ReduceFunction<UserViewCountPerWindow> {
        @Override
        public UserViewCountPerWindow reduce(UserViewCountPerWindow value1, UserViewCountPerWindow value2) throws Exception {
            value1.count = value1.count + value2.count;
            return value1;
        }
    }

    public static class MyMapFun implements MapFunction<Event, UserViewCountPerWindow> {
        @Override
        public UserViewCountPerWindow map(Event value) throws Exception {
            return new UserViewCountPerWindow(value.username, 1L, 0L, 0L);
        }
    }

    public static class EtlStream implements FlatMapFunction<Event, Event> {
        private Random random = new Random();

        @Override
        public void flatMap(Event event, Collector<Event> out) throws Exception {
            if ("Mary".equals(event.username)) {
                int num = (random.nextInt(10) + 1) * 1000;
                for (int i = 0; i < num; i++) {
                    out.collect(event);
                }
            } else {
                out.collect(event);
            }
        }
    }
}
