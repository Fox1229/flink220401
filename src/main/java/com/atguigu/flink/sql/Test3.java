package com.atguigu.flink.sql;

import com.atguigu.flink.datastreamapi.transform.ClickSource;
import com.atguigu.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class Test3 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Event>() {
                                            @Override
                                            public long extractTimestamp(Event element, long recordTimestamp) {
                                                return element.ts;
                                            }
                                        })
                );

        // 创建环境
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(
                env,
                EnvironmentSettings.newInstance().inStreamingMode().build()
        );

        // 将数据转换为动态表
        Table table = streamTableEnvironment
                .fromDataStream(
                        stream,
                        $("username"),
                        $("url"),
                        $("processTime").proctime()
                );

        // 注册临时视图
        streamTableEnvironment.createTemporaryView("clicks", table);

        Table result = streamTableEnvironment
                .sqlQuery(
                        "SELECT username, " +
                                "COUNT(username) as cnt," +
                                "TUMBLE_START(processTime, INTERVAL '5' SECONDS) as start_time," +
                                "TUMBLE_END(processTime, INTERVAL '5' SECONDS) as end_time " +
                                "FROM clicks " +
                                "GROUP BY username, tumble(processTime, INTERVAL '5' SECONDS)"
                );

        // 将动态表转换为数据流
        streamTableEnvironment.toChangelogStream(result).print();

        env.execute();
    }
}
