package com.atguigu.flink.window;

import com.atguigu.flink.datastreamapi.transform.ClickSource;
import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.pojo.UserViewCountPerWindow;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 每个用户在每个5s滚动窗口浏览次数
 */
public class UserViewCountFiveSecondsWindowTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .keyBy(r -> r.username)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new WindowResult())
                .print();

        env.execute();
    }

    // <输入泛型，输入泛型，key的泛型，窗口泛型>
    public static class WindowResult extends ProcessWindowFunction<Event, UserViewCountPerWindow, String, TimeWindow> {

        // 窗口结束时触发，迭代器中保存本个窗口的所有信息
        @Override
        public void process(String username, ProcessWindowFunction<Event, UserViewCountPerWindow, String, TimeWindow>.Context context, Iterable<Event> elements, Collector<UserViewCountPerWindow> out) throws Exception {

            // 获取迭代器中的数据量
            // 方式一
            // long count = 0L;
            // for(Event e : elements) count += 1L;
            out.collect(
                    new UserViewCountPerWindow(
                            username,
                            elements.spliterator().estimateSize(), // 方式二
                            context.window().getStart(),
                            context.window().getEnd()
                    )
            );
        }
    }
}
