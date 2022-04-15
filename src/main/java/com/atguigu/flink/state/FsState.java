package com.atguigu.flink.state;

import com.atguigu.flink.datastreamapi.transform.ClickSource;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FsState {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 每隔10s保存一次检查点
        env.enableCheckpointing(10 * 1000L);
        // 设置检查点文件的文件夹路径
        env.setStateBackend(new FsStateBackend("file:/D:\\developer\\idea workspace\\flink220401\\src\\main\\resources\\chpts"));
        CheckpointConfig config = env.getCheckpointConfig();
        // 当进行保存检查点操作是，超过该时间不进行
        config.setCheckpointTimeout(60000L);
        // 检查点模式，保证检查点一致性级别
        config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 两个检查点之间的最小时间间隔
        config.setMinPauseBetweenCheckpoints(500L);
        // 某个算子最大的检查点个数
        config.setMaxConcurrentCheckpoints(1);
        // 不对齐的检查点操作
        // 减少反压是保存检查点的时间
        // 触发前提：CheckpointingMode.EXACTLY_ONCE && 最大的检查点个数为1
        config.enableUnalignedCheckpoints();
        // 取消作业时是否删除检查点
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 检查点保存允许失败的次数
        config.setTolerableCheckpointFailureNumber(0);

        env
                .addSource(new ClickSource())
                .print();

        env.execute();
    }
}
