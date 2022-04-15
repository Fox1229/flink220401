package com.atguigu.flink.watermark2;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * 保存最近一次的偏移量
 */
public class SaveOffsetTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(10 * 1000L);

        env
                .addSource(new CounterSource())
                .print();

        env.execute();
    }

    public static class CounterSource extends RichParallelSourceFunction<Long> implements CheckpointedFunction {

        private boolean running = true;
        private long offset = 0L;

        @Override
        public void run(SourceContext<Long> ctx) throws Exception {

            final Object lock = ctx.getCheckpointLock();

            while (running) {
                synchronized (lock) {
                    ctx.collect(offset);
                    offset += 1L;
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        // CheckPointedFunction
        // operator state
        private ListState<Long> state;

        // program start
        // or recovery
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            state = context.getOperatorStateStore().getListState(
                    new ListStateDescriptor<Long>(
                            "state",
                            LongSerializer.INSTANCE
                    )
            );

            // only one offset in ListState
            for (long l : state.get()) {
                offset = l;
            }
        }

        // when source receive checkpoint barrier
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            state.clear();
            // store offset
            // garantee that only one offset in ListState
            state.add(offset);
        }
    }
}
