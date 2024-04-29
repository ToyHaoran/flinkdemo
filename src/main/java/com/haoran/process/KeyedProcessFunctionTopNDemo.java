package com.haoran.process;


import com.haoran.bean.WaterSensor;
import com.haoran.functions.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.*;

public class KeyedProcessFunctionTopNDemo {
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


        // 最近10秒=窗口长度， 每5秒输出=滑动步长
        /**
         * TODO 思路二： 使用 KeyedProcessFunction实现
         * 1、按照vc做keyby，开窗，分别count
         *    ==》 增量聚合，计算 count
         *    ==》 全窗口，对计算结果 count值封装 ，  带上 窗口结束时间的 标签
         *          ==》 为了让同一个窗口时间范围的计算结果到一起去
         *
         * 2、对同一个窗口范围的count值进行处理： 排序、取前N个
         *    =》 按照 windowEnd做keyby
         *    =》 使用process，来一条调用一次，需要先存，分开存，用HashMap,key=windowEnd,value=List
         *      =》 使用定时器，对 存起来的结果 进行 排序、取前N个
         */

        // 1. 按照vc分组、开窗、聚合 (增量计算+全量打标签)
        SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> windowAgg = sensorDS
                .keyBy(sensor -> sensor.getVc())
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(
                        new AggregateFunction<WaterSensor, Integer, Integer>() {
                            @Override
                            public Integer createAccumulator() {return 0;}
                            @Override
                            public Integer add(WaterSensor value, Integer accumulator) {
                                return accumulator + 1;
                            }
                            @Override
                            public Integer getResult(Integer accumulator) {
                                return accumulator;
                            }
                            @Override
                            public Integer merge(Integer a, Integer b) {return null;}
                        },
                        // 泛型(count值 输入类型，(vc,count,windowEnd)输出类型，vc key类型，窗口类型)
                        new ProcessWindowFunction<Integer, Tuple3<Integer, Integer, Long>, Integer, TimeWindow>(){
                            @Override
                            public void process(Integer key, Context context, Iterable<Integer> elements, Collector<Tuple3<Integer, Integer, Long>> out) throws Exception {
                                // 迭代器里面只有一条数据，next一次即可
                                Integer count = elements.iterator().next();
                                long windowEnd = context.window().getEnd();  // 开窗聚合后，就是普通的流，没有了窗口信息，需要自己打上窗口的标记windowEnd
                                out.collect(Tuple3.of(key, count, windowEnd));
                            }
                        });
        // 2. 按照窗口标签(窗口结束时间)keyBy，保证同一个窗口时间范围的结果，到一起去。排序、取TopN
        windowAgg.keyBy(r -> r.f2)
                .process(new TopN(2))
                .print();
        env.execute();
    }

    public static class TopN extends KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String> {
        // 存不同窗口的 统计结果，key=windowEnd，value=list数据
        private Map<Long, List<Tuple3<Integer, Integer, Long>>> dataListMap;
        private int threshold;  // 要取的Top数量
        public TopN(int threshold) {
            this.threshold = threshold;
            dataListMap = new HashMap<>();
        }
        @Override
        public void processElement(Tuple3<Integer, Integer, Long> value, Context ctx, Collector<String> out) throws Exception {
            // 进入到该窗口，只是一条数据，要排序，得到齐才行 ===》 存起来，不同窗口分开存
            // 1. 存到HashMap中
            Long windowEnd = value.f2;
            if (dataListMap.containsKey(windowEnd)) {
                // 1.1 包含vc，不是该vc的第一条，直接添加到List中
                dataListMap.get(windowEnd).add(value);
            } else {
                // 1.1 不包含vc，是该vc的第一条，需要初始化list
                List<Tuple3<Integer, Integer, Long>> dataList = new ArrayList<>();
                dataList.add(value);
                dataListMap.put(windowEnd, dataList);
            }
            // 2. 注册一个定时器， windowEnd+1ms即可（
            // 同一个窗口范围，应该同时输出，只不过是一条一条调用processElement方法，只需要延迟1ms即可
            ctx.timerService().registerEventTimeTimer(windowEnd + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            // 定时器触发，同一个窗口范围的计算结果攒齐了，开始 排序、取TopN
            Long windowEnd = ctx.getCurrentKey();
            // 1. 排序
            List<Tuple3<Integer, Integer, Long>> dataList = dataListMap.get(windowEnd);
            dataList.sort((o1, o2) -> o2.f1 - o1.f1);
            // 2. 取TopN
            StringBuilder outStr = new StringBuilder();
            // 遍历 排序后的 List，取出前 threshold 个， 考虑可能List不够2个的情况  ==》 List中元素的个数 和 2 取最小值
            for (int i = 0; i < Math.min(threshold, dataList.size()); i++) {
                Tuple3<Integer, Integer, Long> vcCount = dataList.get(i);
                outStr.append("Top" + (i + 1) + "\n");
                outStr.append("vc=" + vcCount.f0 + "\n");
                outStr.append("count=" + vcCount.f1 + "\n");
                outStr.append("窗口结束时间=" + vcCount.f2 + "\n");
                outStr.append("================================\n");
            }
            // 用完的List，及时清理，节省资源
            dataList.clear();
            out.collect(outStr.toString());
        }
    }
}
