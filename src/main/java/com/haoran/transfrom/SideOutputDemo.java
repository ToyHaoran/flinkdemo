package com.haoran.transfrom;

import com.haoran.bean.WaterSensor;
import com.haoran.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


public class SideOutputDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

        // 1 创建OutputTag对象 (标签名, 放入侧输出流中的 数据类型)
        OutputTag<WaterSensor> s1Tag = new OutputTag<>("s1", Types.POJO(WaterSensor.class));
        OutputTag<WaterSensor> s2Tag = new OutputTag<>("s2", Types.POJO(WaterSensor.class));

        // 2 使用process算子
        SingleOutputStreamOperator<WaterSensor> process = sensorDS
                .process(new ProcessFunction<WaterSensor, WaterSensor>() {
                             @Override
                             public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) {
                                 String id = value.getId();
                                 if ("s1".equals(id)) {  // 如果是s1，放到侧输出流s1中
                                     ctx.output(s1Tag, value);  // 3 调用ctx.output，将数据放入侧输出流
                                 } else if ("s2".equals(id)) {
                                     ctx.output(s2Tag, value);
                                 } else {  // 非s1、s2的数据，放到主流中
                                     out.collect(value);
                                 }
                             }
                         }
                );
        // 4 从主流中，根据标签 获取 侧输出流
        SideOutputDataStream<WaterSensor> s1 = process.getSideOutput(s1Tag);
        SideOutputDataStream<WaterSensor> s2 = process.getSideOutput(s2Tag);
        // 打印主流
        process.print("主流-非s1、s2");
        // 打印 侧输出流
        s1.printToErr("s1");
        s2.printToErr("s2");
        env.execute();
    }
}