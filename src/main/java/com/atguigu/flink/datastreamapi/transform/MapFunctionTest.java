package com.atguigu.flink.datastreamapi.transform;

import com.atguigu.flink.datastreamapi.pojo.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class MapFunctionTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .map(new MapFunction<Event, String>() {
                    @Override
                    public String map(Event value) throws Exception {
                        return value.username;
                    }
                }).print("匿名类实现map");

        env
                .addSource(new ClickSource())
                .map(new MyMap())
                .print("外部类实现map");

        env
                .addSource(new ClickSource())
                .map(e -> e.username)
                .print("lambda实现map");

        env
                .addSource(new ClickSource())
                .flatMap(new FlatMapFunction<Event, String>() {
                    @Override
                    public void flatMap(Event value, Collector<String> out) throws Exception {
                        out.collect(value.username);
                    }
                }).print("flatMap实现map");

        env.execute();
    }

    public static class MyMap implements MapFunction<Event, String> {
        @Override
        public String map(Event value) throws Exception {
            return value.username;
        }
    }
}
