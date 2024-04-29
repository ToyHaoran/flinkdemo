package com.haoran.process;

import com.haoran.bean.WaterSensor;
import com.haoran.functions.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class KeyedProcessTimerDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("hadoop102", 7777)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((element, ts) -> element.getTs() * 1000L)
                );

        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(sensor -> sensor.getId());
        SingleOutputStreamOperator<String> process = sensorKS.process(
                new KeyedProcessFunction<String, WaterSensor, String>() {
                    // 来一条数据调用一次，处理数据的核心逻辑。
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        // 获取当前数据的key
                        String currentKey = ctx.getCurrentKey();
                        // 定时器注册
                        TimerService timerService = ctx.timerService();
                        // 1 事件时间的案例
                        Long currentEventTime = ctx.timestamp(); // 数据中提取出来的事件时间
                        timerService.registerEventTimeTimer(5000L);
                        System.out.println("当前key=" + currentKey + ",当前时间=" + currentEventTime + ",注册了一个5s的定时器");

                        // 2 处理时间的案例
                        long currentTs = timerService.currentProcessingTime();
                        timerService.registerProcessingTimeTimer(currentTs + 5000L);
                        System.out.println("当前key=" + currentKey + ",当前时间=" + currentTs + ",注册了一个5s后的定时器");
                    }


                    // 时间进展到定时器注册的时间，调用该方法。
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        String currentKey = ctx.getCurrentKey();
                        System.out.println("key=" + currentKey + "现在时间是" + timestamp + "定时器触发");
                    }
                }
        );
        process.print();
        env.execute();
    }
}
/**
 * TODO 定时器
 * 1、keyed才有
 * 2、事件时间定时器，通过watermark来触发的
 *    watermark >= 注册的时间
 *    注意： watermark = 当前最大事件时间 - 等待时间 -1ms， 因为 -1ms，所以会推迟一条数据
 *        比如， 5s的定时器，
 *        如果 等待=3s， watermark = 8s - 3s -1ms = 4999ms,不会触发5s的定时器
 *        需要 watermark = 9s -3s -1ms = 5999ms ，才能去触发 5s的定时器
 * 3、在process中获取当前watermark，显示的是上一次的watermark
 *    =》因为process还没接收到这条数据对应生成的新watermark
 */