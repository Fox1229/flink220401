package com.atguigu.flink.homework;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

public class DataSkew1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Random random = new Random();

        env
                .fromElements(
                        Tuple3.of("a", 1L, 1000L),
                        Tuple3.of("a", 1L, 2000L),
                        Tuple3.of("a", 1L, 3000L),
                        Tuple3.of("a", 1L, 4000L),
                        Tuple3.of("a", 1L, 5000L),
                        Tuple3.of("a", 1L, 6000L),
                        Tuple3.of("a", 1L, 7000L),
                        Tuple3.of("a", 1L, 8000L),
                        Tuple3.of("a", 1L, 9000L),
                        Tuple3.of("a", 1L, 10000L),
                        Tuple3.of("b", 1L, 11000L)
                )
                .map(
                        new MapFunction<Tuple3<String, Long, Long>, Tuple3<String, Long, Long>>() {
                            @Override
                            public Tuple3<String, Long, Long> map(Tuple3<String, Long, Long> in) throws Exception {
                                return Tuple3.of(in.f0 + "-" + random.nextInt(4), in.f1, in.f2);
                            }
                        }
                )
                .keyBy(r -> r.f0)
                .process(new MyAgg())
                .map(
                        new MapFunction<Tuple2<String, Long>, Tuple3<String, Integer, Long>>() {
                            @Override
                            public Tuple3<String, Integer, Long> map(Tuple2<String, Long> in) throws Exception {
                                String[] fields = in.f0.split("-");
                                return Tuple3.of(fields[0], Integer.parseInt(fields[1]), in.f1);
                            }
                        }
                )
                .keyBy(r -> r.f0)
                .process(new MySum())
                .print();

        env.execute();
    }

    public static class MySum extends KeyedProcessFunction<String, Tuple3<String, Integer, Long>, Tuple2<String, Long>> {

        private MapState<Integer, Long> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            mapState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<Integer, Long>("mapState", Types.INT, Types.LONG)
            );
        }

        @Override
        public void processElement(Tuple3<String, Integer, Long> in, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {

            mapState.put(in.f1, in.f2);
            long total = 0L;
            for (Long cnt : mapState.values()) {
                total += cnt;
            }
            out.collect(Tuple2.of(in.f0, total));
        }
    }

    public static class MyAgg extends KeyedProcessFunction<String, Tuple3<String, Long, Long>, Tuple2<String, Long>> {

        private ValueState<Tuple2<String, Long>> sum;
        private ValueState<Long> timer;

        @Override
        public void open(Configuration parameters) throws Exception {
            sum = getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple2<String, Long>>("sum", Types.TUPLE(Types.STRING, Types.LONG))
            );

            timer = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("timer", Types.LONG)
            );
        }

        @Override
        public void processElement(Tuple3<String, Long, Long> in, KeyedProcessFunction<String, Tuple3<String, Long, Long>, Tuple2<String, Long>>.Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {

            if (sum.value() == null) {
                // 首次添加
                sum.update(Tuple2.of(in.f0, 1L));
            } else {
                // 非首次添加
                sum.update(Tuple2.of(in.f0, sum.value().f1 + in.f1));
            }

            // 若没有注册定时器
            if (timer.value() == null) {
                ctx.timerService().registerEventTimeTimer(in.f2 + 5 * 1000L);
                timer.update(in.f2 + 5 * 1000L);
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Tuple3<String, Long, Long>, Tuple2<String, Long>>.OnTimerContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {
            out.collect(
                    Tuple2.of(sum.value().f0, sum.value().f1)
            );

            // 清空定时器
            timer.clear();
        }
    }
}
