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


public class KeyProcessFunMapStateTest {

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
                            public void processElement(Event event, Context ctx, Collector<String> out) throws Exception {

                                // 访问的url
                                String url = event.url;

                                // 需要检查MapState中有没有url这个key
                                // 如果没有，说明用户第一次访问此url
                                if(!mapState.contains(url)) {
                                    // 首次添加
                                    mapState.put(url, 1);
                                } else {
                                    // 非首次添加，更新访问次数
                                    mapState.put(url, mapState.get(url) + 1);
                                }

                                // 获取当前的key即为username
                                String username = ctx.getCurrentKey();

                                // 拼接结果
                                StringBuilder sb = new StringBuilder();
                                sb.append(username).append("{\n");
                                for (String key : mapState.keys()) {
                                    sb
                                            .append("\t")
                                            .append(key)
                                            .append(": ")
                                            .append(mapState.get(key))
                                            .append("\n");
                                }
                                sb.append("}\n");

                                out.collect(sb.toString());
                            }
                        })
                .print();

        env.execute();
    }
}
