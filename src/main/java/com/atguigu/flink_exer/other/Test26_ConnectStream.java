package com.atguigu.flink_exer.other;

import com.atguigu.flink.datastreamapi.transform.ClickSource;
import com.atguigu.flink.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

public class Test26_ConnectStream {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 数据流
        DataStreamSource<Event> dataStream = env.addSource(new ClickSource());
        // 查询流
        DataStreamSource<String> queryStream = env.socketTextStream("hadoop102", 6666);

        dataStream.
                keyBy(r -> r.username)
                // 在广播之前按照将查询流并行度设置为1,保证数据按照顺序广播
                .connect(queryStream.setParallelism(1).broadcast())
                .flatMap(new Query())
                .print();

        env.execute();
    }

    public static class Query implements CoFlatMapFunction<Event, String, Event> {
        private String queryUrl = "";

        @Override
        public void flatMap1(Event event, Collector<Event> out) throws Exception {
            // 处理主流
            if (queryUrl.equals(event.url)) {
                out.collect(event);
            }
        }

        @Override
        public void flatMap2(String value, Collector<Event> out) throws Exception {
            // 处理查询流
            queryUrl = value;
        }
    }
}
