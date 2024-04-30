package com.haoran.state;

import com.haoran.bean.WaterSensor;
import com.haoran.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * TODO 针对每种传感器输出最高的3个水位值
 *
 * @author cjp
 * @version 1.0
 */
public class KeyedListStateDemo {
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
                            ListState<Integer> vcListState;
                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                vcListState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("vcListState", Types.INT));
                            }

                            @Override
                            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                                // 1 来一条，存到list状态里
                                vcListState.add(value.getVc());
                                // 2 从list状态拿出来(Iterable)，拷贝到一个List中，排序，只留3个最大的
                                Iterable<Integer> vcListIt = vcListState.get();
                                // 2.1 拷贝到List中
                                List<Integer> vcList = new ArrayList<>();
                                for (Integer vc : vcListIt) {
                                    vcList.add(vc);
                                }
                                // 2.2 对List进行降序排序
                                vcList.sort((o1, o2) -> o2 - o1);
                                // 2.3 只保留最大的3个(list中的个数一定是连续变大，一超过3就立即清理即可)
                                if (vcList.size() > 3) {
                                    // 将最后一个元素清除（第4个）
                                    vcList.remove(3);
                                }
                                out.collect("传感器id为" + value.getId() + ",最大的3个水位值=" + vcList.toString());
                                // 3.更新list状态
                                vcListState.update(vcList);
                            }
                        }
                )
                .print();

        env.execute();
    }
}
