package com.haoran.state;

import com.haoran.bean.WaterSensor;
import com.haoran.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * TODO 计算每种传感器的平均水位
 *
 * @author cjp
 * @version 1.0
 */
public class KeyedAggregatingStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("hadoop102", 7777)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((element, ts) -> element.getTs() * 1000L)
                );

        sensorDS.keyBy(r -> r.getId())
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {
                            AggregatingState<Integer, Double> vcAvgAggregatingState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                vcAvgAggregatingState = getRuntimeContext()
                                    .getAggregatingState(
                                        new AggregatingStateDescriptor<>(
                                            "vcAvgAggregatingState",
                                            new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {
                                                @Override
                                                public Tuple2<Integer, Integer> createAccumulator() {
                                                    return Tuple2.of(0, 0);
                                                }

                                                @Override
                                                public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> accumulator) {
                                                    return Tuple2.of(accumulator.f0 + value, accumulator.f1 + 1);
                                                }

                                                @Override
                                                public Double getResult(Tuple2<Integer, Integer> accumulator) {
                                                    return accumulator.f0 * 1D / accumulator.f1;
                                                }

                                                @Override
                                                public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                                                    // return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
                                                    return null;
                                                }
                                            },
                                            Types.TUPLE(Types.INT, Types.INT))
                                    );
                            }

                            @Override
                            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                                // 将 水位值 添加到 聚合状态中
                                vcAvgAggregatingState.add(value.getVc());
                                // 从 聚合状态中 获取结果
                                Double vcAvg = vcAvgAggregatingState.get();
                                out.collect("传感器id为" + value.getId() + ",平均水位值=" + vcAvg);
                            }
                        }
                )
                .print();

        env.execute();
    }
}