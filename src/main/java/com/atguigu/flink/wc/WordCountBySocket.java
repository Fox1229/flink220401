package com.atguigu.flink.wc;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 从socket读取数据
 */
public class WordCountBySocket {

    public static void main(String[] args) throws Exception {

        // 获取flink的执行环境的上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为1
        env.setParallelism(1);

        // 从socket读取数据
        // 泛型是String，说明source算子读取并输出的数据类型是String
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        // map操作，"hello world" => (hello, 1) (world, 1)
        // 由于这里要将数据流中的每个字符串转换成1个或者多个元组Tuple
        // 所以使用flatMap算子
        // 使用匿名类的方式来定义flatMap的计算逻辑
        // 第一个泛型：输入到flatMap算子的事件类型
        // 第二个泛型：flatMap输出的事件类型
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMapStream = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            // 第一个参数是输入事件
            // 第二个参数是集合Collector类型，用来收集flatMap要发送的数据
            @Override
            public void flatMap(String in, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] fields = in.split(" ");
                for (String field : fields) {
                    // 转换成元组并收集到集合中
                    // flink会自动的将集合中的数据发送到下游算子
                    out.collect(Tuple2.of(field, 1));
                }
            }
        });

        // shuffle操作
        // 将相同key的数据分到一个组来进行处理
        // 第一个泛型：输入事件的类型
        // 第二个泛型：key的泛型
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = flatMapStream.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> in) throws Exception {
                // 将元组的f0字段指定为输入事件的key
                return in.f0;
            }
        });

        // reduce操作
        // 针对元组的f1字段进行聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum("f1");

        // 打印输出结果
        result.print();

        // 执行环境
        env.execute();
    }
}