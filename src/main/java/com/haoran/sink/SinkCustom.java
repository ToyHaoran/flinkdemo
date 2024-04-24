package com.haoran.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;

public class SinkCustom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> sensorDS = env.fromElements("s1,1L,1", "s1,11L,1", "s2,2L,2");
        sensorDS.addSink(new MySink());
        env.execute();
    }
    public static class MySink extends RichSinkFunction<String> {
        Connection conn = null;
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 在这里 创建连接
        }
        @Override
        public void close() throws Exception {
            super.close();
            // 做一些清理、销毁连接
        }
        @Override
        public void invoke(String value, Context context) throws Exception {
            // sink的核心逻辑：来一条数据，调用一次，不要在这里创建 连接对象
        }
    }
}
