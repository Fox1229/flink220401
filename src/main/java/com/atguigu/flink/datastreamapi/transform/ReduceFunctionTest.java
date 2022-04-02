package com.atguigu.flink.datastreamapi.transform;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * 使用reduce同时计算（最大值， 最小值， 总和）
 */
public class ReduceFunctionTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements(1, 3, 2, 5, 7, 2, 5, 4, 3, 2, 9)
                .map(r -> Tuple3.of(r, r, r))
                .returns(new TypeHint<Tuple3<Integer, Integer, Integer>>() {})
                // 将所有数据路由到同一个slot
                .keyBy(r -> 1)
                .reduce(new ReduceFunction<Tuple3<Integer, Integer, Integer>>() {
                    @Override
                    public Tuple3<Integer, Integer, Integer> reduce(Tuple3<Integer, Integer, Integer> t1, Tuple3<Integer, Integer, Integer> t2) throws Exception {
                        return Tuple3.of(
                                Math.min(t1.f0, t2.f0),
                                Math.max(t1.f1, t2.f1),
                                t1.f2 + t2.f2
                        );
                    }
                })
                .print();


        env.execute();
    }
}
