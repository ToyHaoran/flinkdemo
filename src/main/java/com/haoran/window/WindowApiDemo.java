package com.haoran.window;

import com.haoran.bean.WaterSensor;
import com.haoran.functions.WaterSensorMapFunction;
import com.haoran.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class WindowApiDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("hadoop102", 7777)
                .map(new WaterSensorMapFunction());


        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(sensor -> sensor.getId());

        // 时间窗口
        sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));  // 滚动窗口，窗口长度10s
        sensorKS.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(2)));  // 滑动窗口，窗口长度10s，滑动步长2s
        sensorKS.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)));  // 会话窗口，超时间隔5s

        sensorKS.window(TumblingEventTimeWindows.of(Time.seconds(5)));  // 都同上
        sensorKS.window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(2)));
        sensorKS.window(EventTimeSessionWindows.withGap(Time.seconds(5)));

        // 计数窗口
        sensorKS.countWindow(5);  // 滚动窗口，窗口长度=5个元素
        sensorKS.countWindow(5, 2); // 滑动窗口，窗口长度=5个元素，滑动步长=2个元素
        sensorKS.window(GlobalWindows.create());  // 全局窗口，计数窗口的底层，需要自定义的时候才用

        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        // reduce
        SingleOutputStreamOperator<WaterSensor> reduce = sensorWS.reduce(
                (ReduceFunction<WaterSensor>) (value1, value2) -> {
                    System.out.println("调用reduce方法，value1=" + value1 + ",value2=" + value2);
                    return new WaterSensor(value1.getId(), value2.getTs(), value1.getVc() + value2.getVc());
                }
        );

        // aggregate
        SingleOutputStreamOperator<String> aggregate = sensorWS.aggregate(
                // 参数 (IN输入数据的类型，ACC累加器的类型 存储的中间计算结果的类型，OUT输出的类型)
                new AggregateFunction<WaterSensor, Integer, String>() {
                    @Override
                    public Integer createAccumulator() {
                        System.out.println("创建累加器，每个聚合任务只会调用一次");
                        return 0;
                    }

                    @Override  // 聚合逻辑：将输入的元素添加到累加器中
                    public Integer add(WaterSensor value, Integer accumulator) {
                        System.out.println("调用add方法,value="+value);
                        return accumulator + value.getVc();
                    }

                    @Override  // 从累加器获取最终结果，窗口触发时输出
                    public String getResult(Integer accumulator) {
                        System.out.println("调用getResult方法");
                        return accumulator.toString();
                    }
                    @Override  // 只有会话窗口才会用到，合并两个累加器，并将合并后的状态作为一个累加器返回。
                    public Integer merge(Integer a, Integer b) {
                        System.out.println("调用merge方法");
                        return null;
                    }
                }
        );


        env.execute();
    }
}
