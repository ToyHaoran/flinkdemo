package com.haoran.transfrom;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.protocol.types.Field;

public class PartitionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<String> socketDS = env.fromElements("123","456","789","111");

        // 1 随机分区shuffle:
        // random.nextInt(numberOfChannels);  numberOfChannels表示下游算子并行度
        socketDS.shuffle();

        // 2 轮询分区rebalance：Round-Robin算法 将输入流数据平均分配到下游的并行任务中去
        // nextChannelToSendTo = (nextChannelToSendTo + 1) % numberOfChannels;
        // 如果是数据源倾斜的场景，source后，调用rebalance，就可以解决 数据源的 数据倾斜
        socketDS.rebalance();

        // 3 重缩放分区rescale：实现轮询，但分成小团体，发牌人只给自己团体内的所有人轮流发牌。比rebalance更高效。
        // if (++nextChannelToSendTo >= numberOfChannels) nextChannelToSendTo = 0;
        socketDS.rescale();

        // 4 广播 broadcast：发送给下游所有的子任务
        socketDS.broadcast();

        // 5 全局分区 global：全部发往 第一个子任务。
        socketDS.global();

        // 6 keyBy: 按指定key去发送，相同key发往同一个子任务
        // 7 one-to-one: Forward分区器

        // 自定义分区器
        socketDS.partitionCustom(
                (Partitioner<String>) (key, numPartitions) -> Integer.parseInt(key) % numPartitions,
                (KeySelector<String, String>) value -> value
        ).print();

        env.execute();
    }
}
