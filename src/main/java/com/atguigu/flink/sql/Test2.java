package com.atguigu.flink.sql;

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

public class Test2 {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<UserBehavior> dataStream = env
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
                                        })
                );

        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(
                env,
                EnvironmentSettings.newInstance().inStreamingMode().build()
        );

        Table table = streamTableEnvironment
                .fromDataStream(
                        dataStream,
                        $("userId"),
                        $("productId").as("pId"),
                        $("categoryId"),
                        $("type"),
                        $("ts").rowtime()
                );

        // 将表注册成临时视图
        streamTableEnvironment.createTemporaryView("userBehavior", table);

        // 执行查询
        Table result = streamTableEnvironment.sqlQuery(
                "select pId, count(pId) as cnt " +
                        "from userBehavior " +
                        "group by pId"
        );

        streamTableEnvironment.toChangelogStream(result).print();

        env.execute();
    }
}
