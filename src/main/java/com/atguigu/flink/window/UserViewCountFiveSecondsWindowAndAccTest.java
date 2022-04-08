package com.atguigu.flink.window;

import com.atguigu.flink.datastreamapi.transform.ClickSource;
import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.pojo.UserViewCountPerWindow;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class UserViewCountFiveSecondsWindowAndAccTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .keyBy(r -> r.username)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new CountAgg(), new WindowResult())
                .print();

        env.execute();
    }

    /**
     * 累加器
     */
    public static class CountAgg implements AggregateFunction<Event, Long, Long> {

        // 初始化累加器
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        // 每来一条数据 +1
        @Override
        public Long add(Event value, Long accumulator) {
            return accumulator + 1L;
        }

        // 窗口闭合时，将累加器的结果发送给ProcessWindowFunction
        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    // 输入数据的泛型是Long, 也就是getResult的返回值类型
    public static class WindowResult extends ProcessWindowFunction<Long, UserViewCountPerWindow, String, TimeWindow> {
        @Override
        public void process(String username, ProcessWindowFunction<Long, UserViewCountPerWindow, String, TimeWindow>.Context context, Iterable<Long> elements, Collector<UserViewCountPerWindow> out) throws Exception {
            out.collect(
                    new UserViewCountPerWindow(
                            username,
                            // 迭代器只有一个元素，就是getResult的值
                            elements.iterator().next(),
                            context.window().getStart(),
                            context.window().getEnd()
                    )
            );
        }
    }
}
