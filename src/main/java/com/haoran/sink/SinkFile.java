package com.haoran.sink;

import com.haoran.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.time.ZoneId;

public class SinkFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        // DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
        //         (GeneratorFunction<Long, String>) value -> "Number:" + value,
        //         Long.MAX_VALUE,
        //         RateLimiterStrategy.perSecond(1000),
        //         Types.STRING
        // );
        // DataStreamSource<String> dataGen = env.fromSource(
        //         dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator");

        DataStreamSource<String> sensorDS = env.fromElements("s1,1L,1", "s1,11L,1", "s2,2L,2");

        // 输出到文件系统
        FileSink<String> fieSink = FileSink
                // 输出行式存储的文件：行编码.forRowFormat(路径,rowEncoder)。批量编码.forBulkFormat(路径,bulkWriterFactory)
                .<String>forRowFormat(new Path("intput/tmp"), new SimpleStringEncoder<>("UTF-8"))
                // 输出文件的一些配置：文件名的前缀、后缀
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartPrefix("haoran-")
                        .withPartSuffix(".log")
                        .build()
                )
                // 按照目录分桶：每个小时一个目录
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd HH", ZoneId.systemDefault()))
                // 文件滚动策略: 1分钟 或 1m大小
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withRolloverInterval(Duration.ofMinutes(1))
                        .withMaxPartSize(new MemorySize(1024 * 1024))
                        .build()
                )
                .build();

        sensorDS.sinkTo(fieSink);
        env.execute();
    }
}
