package com.haoran.aggreagte;

import com.haoran.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class KeybyDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

        // keyBy：在Flink中，要做聚合，需要先进行分区，即通过keyBy完成分组。(类似SQL中的groupby)
        // keyBy不是转换算子，只是对数据进行逻辑上重分区, 不能设置并行度。
        //   keyBy是对数据分组，保证 相同key的数据 在同一个分区(子任务)
        //   分区：一个子任务可以理解为一个分区，一个分区中可以存在多个分组(key)
        KeyedStream<WaterSensor, String> sensorKS = sensorDS
                .keyBy((KeySelector<WaterSensor, String>) value -> value.getId());  // 通过id分组

        // 简单聚合sum min max minBy maxBy
        // sensorKS.sum(2); 出错，POJO不能传位置索引，只适用于Tuple类型。
        SingleOutputStreamOperator<WaterSensor> result = sensorKS.sum("vc");

        // max：只会取比较字段的最大值，非比较字段保留第一次的值
        // maxBy：取比较字段的最大值，同时非比较字段 取 最大值这条数据的值
        SingleOutputStreamOperator<WaterSensor> result1 = sensorKS.max("vc");
        SingleOutputStreamOperator<WaterSensor> result2 = sensorKS.maxBy("vc");

        result2.print();
        env.execute();
    }
}
