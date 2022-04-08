package com.atguigu.flink.pojo;

import java.sql.Timestamp;

public class UserViewCountPerWindow {

    public String username;
    public Long count;
    public Long windowStartTime;
    public Long windowStopTime;

    public UserViewCountPerWindow() {
    }

    public UserViewCountPerWindow(String username, Long count, Long windowStartTime, Long windowStopTime) {
        this.username = username;
        this.count = count;
        this.windowStartTime = windowStartTime;
        this.windowStopTime = windowStopTime;
    }

    @Override
    public String toString() {
        return "UserViewCountPerWindow{" +
                "username='" + username + '\'' +
                ", count=" + count +
                ", windowStartTime=" + new Timestamp(windowStartTime) +
                ", windowStopTime=" + new Timestamp(windowStopTime) +
                '}';
    }
}
