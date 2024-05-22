package com.atguigu.flink.pojo;

public class IntStatistic {
    public Integer min;
    public Integer max;
    public Integer sum;
    public Integer count;
    public Integer avg;

    public IntStatistic() {
    }

    public IntStatistic(Integer min, Integer max, Integer sum, Integer count, Integer avg) {
        this.min = min;
        this.max = max;
        this.sum = sum;
        this.count = count;
        this.avg = avg;
    }

    @Override
    public String toString() {
        return "IntStatistic{" +
                "min=" + min +
                ", max=" + max +
                ", sum=" + sum +
                ", count=" + count +
                ", avg=" + avg +
                '}';
    }
}
