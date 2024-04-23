package com.haoran.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Arrays;

public class SourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1 从元素和集合中读
        DataStreamSource<Integer> data = env
                // .fromElements(1,2,33); // 从元素读
                .fromCollection(Arrays.asList(1, 22, 3));  // 从集合读

        // 2 从文件读(需要文件连接器)
        FileSource<String> fileSource = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path("input/word.txt"))
                .build();
        DataStreamSource<String> data2 = env
                .fromSource(fileSource, WatermarkStrategy.noWatermarks(), "filesource");

        // 3 从Socket读取无界数据 (用于测试)
        DataStreamSource<String> data3 = env.socketTextStream("localhost", 7777);

        // 4 从Kafka读取数据(需要kafka连接器)
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop100:9092,hadoop101:9092,hadoop102:9092") // 指定kafka节点的地址和端口
                .setGroupId("haoran")  // 指定消费者组的id
                .setTopics("topic_1")   // 指定消费的 Topic
                .setValueOnlyDeserializer(new SimpleStringSchema()) // 只对value反序列化
                .setStartingOffsets(OffsetsInitializer.latest())  // flink消费kafka的策略 (latest从最新消费) (earliest从最早消费)
                .build();
        DataStreamSource<String> data4 = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkasource");

        // 5 从数据生成器读取数据，DataGen连接器生成一些随机数，用于在没有数据源的时候，进行流任务的测试以及性能测试等。
        // 如最大100，并行度为3，则数据均分，每个并行度生成33个，(0-33)(34-66)(67-99)
        env.setParallelism(3);
        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                (GeneratorFunction<Long, String>) value -> "Number:" + value,  // 重写map方法
                100,  // 数字序列最大值+1(从0开始)
                RateLimiterStrategy.perSecond(5),  // 限速策略，如每秒生成几条数据
                Types.STRING  // 返回的类型
        );
        DataStreamSource<String> data5 = env
                .fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator");

        data5.print();
        env.execute();
    }
}
