package com.atguigu.flink.watermark2;

import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;

public class TwoPCTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(10 * 1000L);

        env
                .addSource(
                        new SourceFunction<Long>() {

                            private Boolean running = true;
                            private Random random = new Random();

                            @Override
                            public void run(SourceContext<Long> ctx) throws Exception {

                                while (running) {
                                    ctx.collect(random.nextLong());
                                    Thread.sleep(1000L);
                                }
                            }

                            @Override
                            public void cancel() {
                                running = false;
                            }
                        }
                )
                .addSink(new TransactionFileSink());

        env.execute();
    }

    public static class TransactionFileSink extends TwoPhaseCommitSinkFunction<Long, String, Void> {

        private BufferedWriter transactionWriter;

        public TransactionFileSink() {
            super(StringSerializer.INSTANCE, VoidSerializer.INSTANCE);
        }

        /**
         * 将每条数据写入缓冲区
         */
        @Override
        protected void invoke(String transaction, Long value, Context context) throws Exception {
            transactionWriter.write(value.toString());
            transactionWriter.write("\n");
        }

        @Override
        protected String beginTransaction() throws Exception {

            long currentTimeMillis = System.currentTimeMillis();
            int subtask = getRuntimeContext().getIndexOfThisSubtask();
            String transactionFileName = currentTimeMillis + "-" + subtask;
            Path tmpFilePath = Paths.get("D:\\developer\\idea workspace\\flink220401\\src\\main\\resources\\mychkpts\\" + transactionFileName);
            transactionWriter = Files.newBufferedWriter(tmpFilePath);
            System.out.println("==========开始事务==========");
            return transactionFileName;
        }

        /**
         * 预提交
         */
        @Override
        protected void preCommit(String transaction) throws Exception {
            transactionWriter.flush();
            transactionWriter.close();
        }

        /**
         * 提交操作
         * 将临时文件更名
         */
        @Override
        protected void commit(String transactionFileName) {

            Path tmpFilePath = Paths.get("D:\\developer\\idea workspace\\flink220401\\src\\main\\resources\\mychkpts\\" + transactionFileName);
            // 判断临时路径是否存在
            if (Files.exists(tmpFilePath)) {
                Path commitPath = Paths.get("D:\\developer\\idea workspace\\flink220401\\src\\main\\resources\\success\\" + transactionFileName);
                try {
                    Files.move(tmpFilePath, commitPath);
                    System.out.println("==========提交事务==========");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        /**
         * 回滚操作
         * 删除临时文件
         */
        @Override
        protected void abort(String transactionFileName) {

            Path tmpFilePath = Paths.get("D:\\developer\\idea workspace\\flink220401\\src\\main\\resources\\mychkpts\\" + transactionFileName);
            if (Files.exists(tmpFilePath)) {
                try {
                    Files.delete(tmpFilePath);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
