package com.atguigu.flink.watermark1;

import com.atguigu.flink.datastreamapi.transform.ClickSource;
import com.atguigu.flink.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

public class SearchStreamTest1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> clickStream = env.addSource(new ClickSource());

        DataStreamSource<String> searchStream = env.socketTextStream("hadoop102", 9999);

        clickStream
                .keyBy(r -> r.username)
                .connect(searchStream.setParallelism(1).broadcast())
                .flatMap(new Query())
                .print();

        env.execute();
    }

    public static class Query implements CoFlatMapFunction<Event, String, Event> {

        private String queryElement;

        @Override
        public void flatMap1(Event event, Collector<Event> out) throws Exception {
            if(event.url.equals(queryElement)) {
                out.collect(event);
            }
        }

        @Override
        public void flatMap2(String value, Collector<Event> out) throws Exception {
            queryElement = value;
        }
    }
}
