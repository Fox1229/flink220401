package com.atguigu.flink.sql;

import com.atguigu.flink.pojo.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class HotCategoryTop3 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<UserBehavior> stream = env
                .readTextFile("D:\\developer\\idea workspace\\flink220401\\data\\UserBehavior.csv")
                .map(
                        new MapFunction<String, UserBehavior>() {
                            @Override
                            public UserBehavior map(String value) throws Exception {
                                String[] fields = value.split(",");
                                return new UserBehavior(fields[0], fields[1], fields[2], fields[3], Long.parseLong(fields[4]) * 1000L);
                            }
                        }
                )
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

        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(
                env,
                EnvironmentSettings.newInstance().inStreamingMode().build()
        );

        // 将数据流转换为动态表
        Table table = streamTableEnvironment
                .fromDataStream(
                        stream,
                        $("productId").as("pId"),
                        $("ts").rowtime()
                );

        streamTableEnvironment.createTemporaryView("userBehavior", table);

        // stream
        // .KeyBy(r -> r.productId)
        // .window()
        // .aggregate(new ProductAcc(), new ProductFun())
        // count底层不是累加器，而是使用全窗口聚合，维护了所有的数据，占用内存
        String innerSql = "SELECT pId, count(pId) cnt, " +
                "HOP_END(ts, INTERVAL '5' minutes, INTERVAL '1' hours) end_time " +
                "FROM userBehavior " +
                "GROUP BY pId, HOP(ts, INTERVAL '5' minutes, INTERVAL '1' hours)";

        String outerSql = "SELECT * from (" +
                "SELECT *, " +
                "row_number() over(partition by end_time order by cnt desc) rn " +
                "from (" + innerSql + ") " +
                ") t1 " +
                "where rn <= 3";

        Table result = streamTableEnvironment.sqlQuery(outerSql);

        streamTableEnvironment.toChangelogStream(result).print();

        env.execute();
    }
}
