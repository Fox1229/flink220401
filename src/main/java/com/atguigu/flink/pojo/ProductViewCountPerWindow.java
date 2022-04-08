package com.atguigu.flink.pojo;

import java.sql.Timestamp;

public class ProductViewCountPerWindow {

    public String productId;
    public Long count;
    public Long windowStartTime;
    public Long windowEndTime;

    public ProductViewCountPerWindow() {
    }

    public ProductViewCountPerWindow(String productId, Long count, Long windowStartTime, Long windowEndTime) {
        this.productId = productId;
        this.count = count;
        this.windowStartTime = windowStartTime;
        this.windowEndTime = windowEndTime;
    }

    @Override
    public String toString() {
        return "ProductViewCountPerWindow{" +
                "productId='" + productId + '\'' +
                ", count=" + count +
                ", windowStartTime=" + new Timestamp(windowStartTime) +
                ", windowEndTime=" + new Timestamp(windowEndTime) +
                '}';
    }
}
