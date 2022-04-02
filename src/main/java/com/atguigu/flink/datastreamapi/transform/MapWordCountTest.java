package com.atguigu.flink.datastreamapi.transform;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapWordCountTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements(
                        "hello", "hello", "world"
                )
                .map(word -> Tuple2.of(word, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(word -> word.f0)
                // reduce 输入和输出类型一致，无需对输出类型做注解，能够根据输入类型推断
                .reduce(
                        (Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) -> Tuple2.of(t1.f0, t1.f1 + t2.f1)
                )
                .print();

        env.execute();
    }
}
