package com.atguigu.flink.homework;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;

public class StateMachine1 {

    public static void main(String[] args) {

        HashMap<Tuple2<String, Boolean>, String> map = new HashMap<>();
        map.put(Tuple2.of("S1", true), "S2");
        map.put(Tuple2.of("S2", true), "S2");

        String num = "12345";
        String currentState = "S1";
        boolean op = true;
        char[] chars = num.toCharArray();

        for (int i = 0; i < chars.length; i++) {
            char tmp = chars[i];
            if (tmp < 48 | tmp > 57) {
                throw new RuntimeException("非法数据");
            } else {
                currentState = map.get(Tuple2.of(currentState, op));
            }
        }

        if (currentState.equals("S2")) {
            System.out.println(num);
        }
    }
}
