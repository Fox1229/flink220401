package com.atguigu.flink.pojo;

public class SensorReading {

    public String sensorId;
    public Double temp;

    public SensorReading() {
    }

    public SensorReading(String sensorId, Double temp) {
        this.sensorId = sensorId;
        this.temp = temp;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "sensorId='" + sensorId + '\'' +
                ", temp=" + temp +
                '}';
    }
}
