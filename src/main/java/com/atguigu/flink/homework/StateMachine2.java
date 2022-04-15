package com.atguigu.flink.homework;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;

public class StateMachine2 {

    public static void main(String[] args) {

        HashMap<Tuple2<String, Character>, String> map = new HashMap<>();
        map.put(Tuple2.of("S1", '0'), "S2");
        map.put(Tuple2.of("S1", '1'), "S2");
        map.put(Tuple2.of("S1", '2'), "S2");
        map.put(Tuple2.of("S1", '3'), "S2");
        map.put(Tuple2.of("S1", '4'), "S2");
        map.put(Tuple2.of("S1", '5'), "S2");
        map.put(Tuple2.of("S1", '6'), "S2");
        map.put(Tuple2.of("S1", '7'), "S2");
        map.put(Tuple2.of("S1", '8'), "S2");
        map.put(Tuple2.of("S1", '9'), "S2");
        map.put(Tuple2.of("S2", '0'), "S2");
        map.put(Tuple2.of("S2", '1'), "S2");
        map.put(Tuple2.of("S2", '2'), "S2");
        map.put(Tuple2.of("S2", '3'), "S2");
        map.put(Tuple2.of("S2", '4'), "S2");
        map.put(Tuple2.of("S2", '5'), "S2");
        map.put(Tuple2.of("S2", '6'), "S2");
        map.put(Tuple2.of("S2", '7'), "S2");
        map.put(Tuple2.of("S2", '8'), "S2");
        map.put(Tuple2.of("S2", '9'), "S2");

        String num = "123.2";
        String currentState = "S1";
        String nextState = null;

        for (int i = 0; i < num.length(); i++) {
            nextState = map.get(Tuple2.of(currentState, num.charAt(i)));
            if (nextState == null) {
                throw new RuntimeException("非法数据");
            }

            currentState = nextState;
        }

        if (nextState.equals("S2")) {
            System.out.println(num);
        }
    }
}
