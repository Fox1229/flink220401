package com.atguigu.flink.datastreamapi.richfun;

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.datastreamapi.transform.ClickSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * 幂等写入数据库
 */
public class RichFunToMysqlTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .addSink(new MyJdbc());

        env.execute();
    }

    public static class MyJdbc extends RichSinkFunction<Event> {

        private Connection conn = null;
        private PreparedStatement insertPs = null;
        private PreparedStatement updatePs = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            conn = DriverManager.getConnection(
                    "jdbc:mysql://hadoop102:3306/flink?useSSL=false",
                    "root",
                    "123456"
            );

            insertPs = conn.prepareStatement("insert into user_action(username, url) values(?, ?)");
            updatePs = conn.prepareStatement("update user_action set url = ? where username = ?");
        }

        @Override
        public void invoke(Event value, Context context) throws Exception {

            // 每一条数据都会触发调用
            // 更新数据
            updatePs.setString(1, value.url);
            updatePs.setString(2, value.username);
            updatePs.executeUpdate();

            // 数据不存在, 更新失败。插入数据
            if(updatePs.getUpdateCount() == 0) {
                insertPs.setString(1, value.username);
                insertPs.setString(2, value.url);
                insertPs.execute();
            }
        }

        @Override
        public void close() throws Exception {
            insertPs.close();
            updatePs.close();
            conn.close();
        }
    }
}
