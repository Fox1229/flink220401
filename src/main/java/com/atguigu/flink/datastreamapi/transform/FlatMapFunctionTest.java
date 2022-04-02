package com.atguigu.flink.datastreamapi.transform;

import com.sun.org.apache.xalan.internal.xsltc.compiler.util.Type;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlatMapFunctionTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 将T开头的人名原样输出，J开头打印两次，其他不打印
        env
                .fromElements("Tom", "Jerry", "Rose", "Jack")
                .flatMap(new MyFlatMap())
                .print("外部类实现");

        env
                .fromElements("Tom", "Jerry", "Rose", "Jack")
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        if (value.startsWith("T")) {
                            out.collect(value);
                        } else if (value.startsWith("J")) {
                            out.collect(value);
                            out.collect(value);
                        }
                    }
                }).print("匿名类实现");

        env
                .fromElements("Tom", "Jerry", "Rose", "Jack")
                .flatMap((String username, Collector<String> out) -> {
                    if (username.startsWith("T")) {
                        out.collect(username);
                    } else if (username.startsWith("J")) {
                        out.collect(username);
                        out.collect(username);
                    }
                })
                // Collector<String> out 被类型擦除成了 Collector<Object> out
                // 手动进行类型注解
                // .returns(new TypeHint<String>() {})
                .returns(Types.STRING)
                .print("lambda实现");

        env.execute();
    }

    public static class MyFlatMap implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            if (value.startsWith("T")) {
                out.collect(value);
            } else if (value.startsWith("J")) {
                out.collect(value);
                out.collect(value);
            }
        }
    }
}
