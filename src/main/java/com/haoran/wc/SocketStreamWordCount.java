package com.haoran.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * 读取socket文本流
 * 在实际的生产环境中，真正的数据流其实是无界的，有开始却没有结束，这就要求我们需要持续地处理捕获的数据。
 * 1 为了模拟这种场景，可以监听socket端口，然后向该端口不断的发送数据。
 * 2 在Linux环境的主机hadoop102上，启动端口 nc -lk 7777
 * 3 启动程序，然后在端口发送数据进行测试
 *
 * 对于flatMap里传入的Lambda表达式，系统只能推断出返回的是Tuple2类型，而无法得到Tuple2<String, Long>。
 * 只有显式地告诉系统当前的返回类型，才能正确地解析出完整数据。
 */
public class SocketStreamWordCount  {
    public static void main(String[] args) throws Exception {

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 读取文本流：hadoop102表示发送端主机名、7777表示端口号
        DataStreamSource<String> lineStream = env.socketTextStream("hadoop102", 7777);

        // 3. 转换、分组、求和，得到统计结果
        SingleOutputStreamOperator<Tuple2<String, Long>> sum =
                lineStream.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                            String[] words = line.split(" ");

                            for (String word : words) {
                                out.collect(Tuple2.of(word, 1L));
                            }
                        }).returns(Types.TUPLE(Types.STRING, Types.LONG))
                        .keyBy(data -> data.f0)
                        .sum(1);

        // 4. 打印
        sum.print();

        // 5. 执行
        env.execute();

    }
}
