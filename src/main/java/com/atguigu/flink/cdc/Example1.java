package com.atguigu.flink.cdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Example1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        MySqlSource<String> mySqlSource = MySqlSource
                .<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("flink")
                .tableList("flink.flink_test")
                .username("root")
                .password("123456")
                // 指定反序列化的工具
                .deserializer(new JsonDebeziumDeserializationSchema())
                // 指定启动程序时扫描整个binlog
                .startupOptions(StartupOptions.initial())
                .build();

        // 监听binlog，然后直接输出
        env
                .fromSource(
                        mySqlSource,
                        WatermarkStrategy.noWatermarks(),
                        "MySqlSource"
                )
                .print();

        env.execute();
    }
}
