package com.atguigu.flink.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DDLTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnvironment =
                StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().inStreamingMode().build());

        // 定义输入表
        String inputSql = "create table clicks(username string, url string) " +
                "with(" +
                "'connector' = 'filesystem', " +
                "'path' = 'D:\\developer\\idea workspace\\flink220401\\data\\file.csv', " +
                "'format' = 'csv'" +
                ")";
        streamTableEnvironment.executeSql(inputSql);

        // 定义输出表
        String outSql = "create table resultTable(username String, cnt bigint) " +
                "with(" +
                "'connector' = 'print'" +
                ")";
        streamTableEnvironment.executeSql(outSql);

        // 从输入表查询，将查询结果写入输出表
        String result =
                "insert into resultTable " +
                "select username, count(username) cnt " +
                "from clicks " +
                "group by username";
        streamTableEnvironment.executeSql(result);
    }
}
