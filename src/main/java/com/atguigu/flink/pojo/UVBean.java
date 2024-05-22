package com.atguigu.flink.pojo;

public class UVBean {
    // 窗口起始时间
    public String stt;
    // 窗口闭合时间
    public String edt;
    // 当天日期
    public String curDate;
    // 加购独立用户数
    public Long uvCt;

    public UVBean() {
    }

    public UVBean(Long uvCt) {
        this.uvCt = uvCt;
    }

    public UVBean(String stt, String edt, String curDate) {
        this.stt = stt;
        this.edt = edt;
        this.curDate = curDate;
    }

    public UVBean(String stt, String edt, String curDate, Long uvCt) {
        this.stt = stt;
        this.edt = edt;
        this.curDate = curDate;
        this.uvCt = uvCt;
    }

    @Override
    public String toString() {
        return "UVBean{" +
                "stt='" + stt + '\'' +
                ", edt='" + edt + '\'' +
                ", curDate='" + curDate + '\'' +
                ", uvCt=" + uvCt +
                '}';
    }
}
