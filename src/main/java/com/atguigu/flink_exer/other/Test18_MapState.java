package com.atguigu.flink_exer.other;

import com.atguigu.flink.datastreamapi.transform.ClickSource;
import com.atguigu.flink.pojo.Event;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Test18_MapState {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .keyBy(bean -> bean.username)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    private MapState<String, Long> mapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 保存每个页面的访问次数
                        MapStateDescriptor<String, Long> mapStateDesc = new MapStateDescriptor<>("mapStateDesc", Types.STRING, Types.LONG);
                        mapState = getRuntimeContext().getMapState(mapStateDesc);
                    }

                    @Override
                    public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context context, Collector<String> out) throws Exception {
                        // 判断mapState该url是否存在，不存在则赋值为1，存在则累加
                        String url = event.url;
                        if (!mapState.contains(url)) {
                            // 第一条数据到达
                            mapState.put(url, 1L);
                        } else {
                            // 之后数据到达
                            mapState.put(url, mapState.get(url) + 1L);
                        }

                        StringBuilder sb = new StringBuilder();
                        sb.append(event.username).append(" {\n");
                        for (String key : mapState.keys()) {
                            sb
                                    .append("  ")
                                    .append(key)
                                    .append(" => ")
                                    .append(mapState.get(key))
                                    .append(",\n");
                        }
                        sb.append("}\n");

                        out.collect(String.valueOf(sb));
                    }
                })
                .print();

        env.execute();
    }
}
