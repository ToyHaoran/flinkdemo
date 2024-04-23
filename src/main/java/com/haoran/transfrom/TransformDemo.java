package com.haoran.transfrom;

import com.haoran.bean.WaterSensor;
import com.haoran.functions.FilterFunctionImpl;
import com.haoran.functions.MapFunctionImpl;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransformDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

        // 1 map算子：一进一出
        // 简单业务用lambda表达式、匿名内部类
        SingleOutputStreamOperator<String> map = sensorDS.map(sensor -> sensor.getId());
        // 复杂业务用实现类
        SingleOutputStreamOperator<String> map2 = sensorDS.map(new MapFunctionImpl());

        // 2 filter算子：true保留，false过滤掉
        SingleOutputStreamOperator<WaterSensor> filter = sensorDS
                .filter((FilterFunction<WaterSensor>) value -> "s1".equals(value.getId()));

        // 3 flatmap：一进多出，通过Collector来输出，调用几次就输出几条
        SingleOutputStreamOperator<String> flatmap = sensorDS
                .flatMap((FlatMapFunction<WaterSensor, String>) (value, out) -> {
                    if ("s1".equals(value.getId())) {  // 如果是s1，输出vc
                        out.collect(value.getVc().toString());
                    } else if ("s2".equals(value.getId())) {  // 如果是s2，分别输出ts和vc
                        out.collect(value.getTs().toString());
                        out.collect(value.getVc().toString());
                    }
                });

        map.print();
        env.execute();
    }
}
