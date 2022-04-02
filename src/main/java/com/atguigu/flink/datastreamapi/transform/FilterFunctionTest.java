package com.atguigu.flink.datastreamapi.transform;

import com.atguigu.flink.datastreamapi.pojo.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import javax.lang.model.element.ElementVisitor;

public class FilterFunctionTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .filter(new MyFilter())
                .print("外部类");

        env.addSource(new ClickSource())
                .filter(new FilterFunction<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return "Mary".equals(value.username);
                    }
                }).print("匿名类");

        env
                .addSource(new ClickSource())
                .filter(r -> "Bob".equals(r.username))
                .print("lambda");

        env
                .addSource(new ClickSource())
                .flatMap(new FlatMapFunction<Event, Event>() {
                    @Override
                    public void flatMap(Event value, Collector<Event> out) throws Exception {
                        if ("Alice".equals(value.username)) {
                            out.collect(value);
                        }
                    }
                }).print("flatMap实现filter");

        env
                .addSource(new ClickSource())
                .flatMap((Event event, Collector<Event> out) -> {
                    if ("Mary".equals(event.username)) {
                        out.collect(event);
                    }
                }).returns(new TypeHint<Event>() {
                }).print("==> ");

        env.execute();
    }

    public static class MyFilter implements FilterFunction<Event> {
        @Override
        public boolean filter(Event value) throws Exception {
            return "Mary".equals(value.username);
        }
    }
}
