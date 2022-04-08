package com.atguigu.flink.datastreamapi.transform;

import com.atguigu.flink.pojo.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * 自定义数据源
 */
public class ClickSource implements SourceFunction<Event> {

    private Boolean running = true;
    private Random random = new Random();
    private String[] users = {"Mary", "Bob", "Alice", "John", "Liz"};
    private String[] urls = {"./home", "./cart", "./buy", "./prod?id=1"};

    // flink run jobId 会触发run方法的执行
    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        while (running) {
            ctx.collect(new Event(
                    users[random.nextInt(users.length)],
                    urls[random.nextInt(urls.length)],
                    Calendar.getInstance().getTimeInMillis()
            ));

            Thread.sleep(1000L);
        }
    }

    // flink cancel jobId 会触发cancel方法的执行
    @Override
    public void cancel() {
        running = false;
    }
}
