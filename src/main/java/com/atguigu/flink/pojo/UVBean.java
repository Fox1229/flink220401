package com.atguigu.flink.pojo;

public class UVBean {
    // ������ʼʱ��
    public String stt;
    // ���ڱպ�ʱ��
    public String edt;
    // ��������
    public String curDate;
    // �ӹ������û���
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
