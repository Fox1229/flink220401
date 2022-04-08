package com.atguigu.flink.lowapi;

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.datastreamapi.transform.ClickSource;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class KeyProcessFunMapStateTest1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .keyBy(r -> r.username)
                .process(
                        new KeyedProcessFunction<String, Event, String>() {

                            // 定义状态
                            private MapState<String, Integer> mapState;

                            // 初始化状态
                            @Override
                            public void open(Configuration parameters) throws Exception {
                                mapState = getRuntimeContext().getMapState(
                                        new MapStateDescriptor<String, Integer>(
                                                "mapState",
                                                Types.STRING,
                                                Types.INT
                                        )
                                );
                            }

                            @Override
                            public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {

                                // 获取当前访问的url
                                String url = value.url;

                                // 判断是否首次登录
                                if(!mapState.contains(url)) {
                                    // 首次登录
                                    mapState.put(url, 1);
                                } else {
                                    // 非首次登录，访问次数 +1
                                    mapState.put(url, mapState.get(url) + 1);
                                }

                                // 获取当前map的key即为登录用户
                                String username = ctx.getCurrentKey();

                                // 拼接结果
                                StringBuilder sb = new StringBuilder();
                                sb.append(username).append("{\n");
                                // 遍历集合，获取每一个url访问的次数
                                for (String key : mapState.keys()) {
                                    sb.append(key).append(": ").append(mapState.get(key)).append("\n");
                                }
                                sb.append("}\n");

                                // 返回结果
                                out.collect(sb.toString());
                            }
                        })
                .print();

        env.execute();
    }
}
