package com.atguigu.flink.pojo;

import java.sql.Timestamp;

public class Event {

    public String username;
    public String url;
    public Long ts;

    public Event() {
    }

    public Event(String username, String url, Long ts) {
        this.username = username;
        this.url = url;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "Event{" +
                "username='" + username + '\'' +
                ", url='" + url + '\'' +
                ", ts=" + new Timestamp(ts) +
                '}';
    }
}
