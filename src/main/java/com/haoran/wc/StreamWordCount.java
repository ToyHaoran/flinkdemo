package com.haoran.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * 流处理
 * 读取文档words.txt中的数据，并统计每个单词出现的频次。整体思路与之前的批处理非常类似，代码模式也基本一致。
 */
public class StreamWordCount {

    public static void main(String[] args) throws Exception {

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 读取文件
        DataStreamSource<String> lineStream = env.readTextFile("input/words.txt");

        // 3. 转换、分组、求和，得到统计结果
        SingleOutputStreamOperator<Tuple2<String, Long>> sum =
                lineStream.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Long>> out) throws Exception {

                        String[] words = line.split(" ");

                        for (String word : words) {
                            out.collect(Tuple2.of(word, 1L));
                        }
                    }
                }).keyBy(data -> data.f0).sum(1);

        // 4. 打印
        sum.print();

        // 5. 执行
        env.execute();
    }
}

