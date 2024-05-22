package com.atguigu.flink.pojo;

import java.sql.Timestamp;

public class UrlViewCountPerWindow {
    public String url;
    public Long count;
    public Long windowStartTime;
    public Long windowEndTime;

    public UrlViewCountPerWindow() {
    }

    public UrlViewCountPerWindow(String url, Long count, Long windowStartTime, Long windowEndTime) {
        this.url = url;
        this.count = count;
        this.windowStartTime = windowStartTime;
        this.windowEndTime = windowEndTime;
    }

    @Override
    public String toString() {
        return "UrlViewCountPerWindow{" +
                "url='" + url + '\'' +
                ", count=" + count +
                ", windowStartTime=" + new Timestamp(windowStartTime) +
                ", windowEndTime=" + new Timestamp(windowEndTime) +
                '}';
    }
}
