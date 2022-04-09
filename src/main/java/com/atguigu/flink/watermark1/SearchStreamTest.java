package com.atguigu.flink.watermark1;

import com.atguigu.flink.datastreamapi.transform.ClickSource;
import com.atguigu.flink.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * 查询流
 */
public class SearchStreamTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> clickSource = env.addSource(new ClickSource());

        DataStreamSource<String> queryStream = env.socketTextStream("hadoop102", 9999);

        clickSource
                .keyBy(r -> r.username)
                // 广播之前将queryStream的并行度设置为1
                // 保证数据按照顺序广播
                .connect(queryStream.setParallelism(1).broadcast())
                .flatMap(new Query())
                .print();

        env.execute();
    }

    public static class Query implements CoFlatMapFunction<Event, String, Event> {

        private String queryStr = "";

        /**
         * 处理来自clickSource的数据
         */
        @Override
        public void flatMap1(Event event, Collector<Event> out) throws Exception {
            if(event.url.equals(queryStr)) {
                out.collect(event);
            }
        }

        /**
         * 处理来自socket端的数据
         */
        @Override
        public void flatMap2(String value, Collector<Event> out) throws Exception {
            queryStr = value;
        }
    }
}
