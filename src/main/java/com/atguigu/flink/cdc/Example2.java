package com.atguigu.flink.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Example2 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().inStreamingMode().build());

        streamTableEnvironment
                .executeSql("CREATE TABLE aaa (" +
                        "id INT, " +
                        "username STRING," +
                        "url STRING," +
                        "primary key(id) not enforced" +
                        ") WITH (" +
                        "'connector' = 'mysql-cdc'," +
                        "'hostname' = 'hadoop102'," +
                        "'port' = '3306'," +
                        "'username' = 'root'," +
                        "'password' = '123456'," +
                        "'database-name' = 'flink'," +
                        "'table-name' = 'clicks'" +
                        ")");

        streamTableEnvironment.executeSql("select * from aaa").print();

//        env.execute();
    }
}
