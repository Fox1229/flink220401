package com.atguigu.flink.sql;

import com.atguigu.flink.datastreamapi.transform.ClickSource;
import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.pojo.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


import static org.apache.flink.table.api.Expressions.$;

public class Test4 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<UserBehavior> stream = env
                .readTextFile("D:\\developer\\idea workspace\\flink220401\\data\\UserBehavior.csv")
                .map(data -> {
                    String[] fields = data.split(",");
                    return new UserBehavior(fields[0], fields[1], fields[2], fields[3], Long.parseLong(fields[4]) * 1000L);
                })
                .returns(new TypeHint<UserBehavior>() {
                })
                .filter(r -> r.type.equals("pv"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserBehavior>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<UserBehavior>() {
                                            @Override
                                            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                                return element.ts;
                                            }
                                        }
                                )
                );

        // 创建环境
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(
                env,
                EnvironmentSettings.newInstance().inStreamingMode().build()
        );

        // 将数据流转换为动态表
        Table table = streamTableEnvironment
                .fromDataStream(
                        stream,
                        $("userId"),
                        $("productId").as("pId"),
                        $("categoryId").as("cId"),
                        $("type"),
                        $("ts").rowtime()
                );

        streamTableEnvironment.createTemporaryView("userBehavior", table);

        // 查询
        Table result = streamTableEnvironment
                .sqlQuery(
                        "select userId, count(pId)," +
                                "hop_start(ts, interval '5' minutes, interval '1' hours) as start_time," +
                                "hop_end(ts, interval '5' minutes, interval '1' hours) as end_time " +
                                "from userBehavior " +
                                "group by userId, hop(ts, interval '5' minutes, interval '1' hours)"
                );

        streamTableEnvironment.toChangelogStream(result).print();

        env.execute();
    }
}
