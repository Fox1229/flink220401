package com.atguigu.flink.datastreamapi.transform;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceFunctionAvgTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements(1, 3, 2, 5, 7, 2, 5, 4, 3, 2, 9)
                .map(r -> Tuple2.of(r, 1))
                .returns(new TypeHint<Tuple2<Integer, Integer>>() {})
                .keyBy(r -> 1)
                .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> t1, Tuple2<Integer, Integer> t2) throws Exception {
                        return Tuple2.of(
                                t1.f0 + t2.f0,
                                t1.f1 + 1
                        );
                    }
                })
                .map(r -> "sum = " + r.f0 + ", num = " + r.f1 + ", avg = " + r.f0 / r.f1)
                .print();

        env.execute();
    }
}
