package com.haoran.watermark;

import com.haoran.bean.WaterSensor;
import com.haoran.functions.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class WatermarkCustomDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 默认周期200ms，都是周期性生成的
        env.getConfig().setAutoWatermarkInterval(2000);

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1), new WaterSensor("s1", 11L, 11),
                new WaterSensor("s2", 2L, 2), new WaterSensor("s3", 3L, 3)
        );

        // 有序流中的内置水位线设置
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy
                // 1 指定水位线生成器：时间戳单调递增，没有等待时间
                .<WaterSensor>forMonotonousTimestamps()
                // 2 指定时间戳分配器，从数据中提取
                .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (element, recordTimestamp) -> element.getTs() * 1000L);

        // 乱序流中的水位线设置
        WatermarkStrategy<WaterSensor> watermarkStrategy2 = WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))  // 时间戳的最大差值，等待3秒
                .withTimestampAssigner((element, recordTimestamp) -> element.getTs() * 1000L);  // 从数据中提取时间戳

        // 自定义水位线生成器
        WatermarkStrategy<WaterSensor> watermarkStrategy3 = WatermarkStrategy
                // 1 周期性生成
                // .<WaterSensor>forGenerator(ctx -> new MyPeriodWatermarkGenerator<>(3000L))
                // 2 断点式生成
                .<WaterSensor>forGenerator(ctx -> new MyPuntuatedWatermarkGenerator<>(3000L))
                .withTimestampAssigner((element, recordTimestamp) -> element.getTs() * 1000L);

        DataStream<WaterSensor> sensorDSwithWatermark = sensorDS.assignTimestampsAndWatermarks(watermarkStrategy);


        sensorDSwithWatermark
                .keyBy(sensor -> sensor.getId())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))  // 使用 事件时间语义 的窗口
                .process(
                        new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                            @Override
                            public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                                long startTs = context.window().getStart();
                                long endTs = context.window().getEnd();
                                String windowStart = DateFormatUtils.format(startTs, "yyyy-MM-dd HH:mm:ss.SSS");
                                String windowEnd = DateFormatUtils.format(endTs, "yyyy-MM-dd HH:mm:ss.SSS");
                                long count = elements.spliterator().estimateSize();
                                out.collect("key=" + s + "的窗口[" + windowStart + "," + windowEnd + ")包含" + count + "条数据===>" + elements.toString());
                            }
                        }
                )
                .print();

        env.execute();
    }
}
