package com.atguigu.flink.pojo;

public class UserBehavior {

    public String userId;
    public String productId;
    public String categoryId;
    public String type;
    public Long ts;

    public UserBehavior() {
    }

    public UserBehavior(String userId, String productId, String categoryId, String type, Long ts) {
        this.userId = userId;
        this.productId = productId;
        this.categoryId = categoryId;
        this.type = type;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "UserBehavior{" +
                "userId='" + userId + '\'' +
                ", productId='" + productId + '\'' +
                ", categoryId='" + categoryId + '\'' +
                ", type='" + type + '\'' +
                ", ts=" + ts +
                '}';
    }
}
