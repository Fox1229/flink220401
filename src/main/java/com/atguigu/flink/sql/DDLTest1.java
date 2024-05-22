package com.atguigu.flink.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DDLTest1 {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnvironment
                = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().inStreamingMode().build());

        // 输入表
        String inSql = "create table clicks(username String, url String, ts TIMESTAMP(3), " +
                "watermark for ts as ts - interval '3' seconds)" +
                "with(" +
                "'connector' = 'filesystem', " +
                "'path' = 'D:\\developer\\idea workspace\\flink220401\\data\\clicks.csv', " +
                "'format' = 'csv'" +
                ")";
        streamTableEnvironment.executeSql(inSql);

        // 输出表
        String outSql = "create table resultTable(username String, cnt bigint, endTime timestamp(3)) " +
                "with(" +
                "'connector' = 'print'" +
                ")";
        streamTableEnvironment.executeSql(outSql);

        // 执行查询
        String sql =
                "insert into resultTable " +
                "select username, count(username), TUMBLE_END(ts, interval '5' seconds) end_time " +
                "from clicks " +
                "group by username, tumble(ts, interval '5' seconds)";
        streamTableEnvironment.executeSql(sql);
    }
}
