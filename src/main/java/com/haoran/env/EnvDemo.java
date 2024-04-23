package com.haoran.env;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class EnvDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // conf.set(RestOptions.BIND_PORT, "8082");  // webUI端口

        // 1 创建执行环境，是所有Flink程序的基础
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                // .createLocalEnvironment();
                // .createRemoteEnvironment("hadoop102", 8081, "path/to/jarFile.jar")
                .getExecutionEnvironment(conf); // 自动识别环境(远程集群 or idea本地环境)，conf可修改一些参数

        // 2 执行模式，流批一体，一般提交时参数指定，更加灵活：-Dexecution.runtime-mode=BATCH
        // env.setRuntimeMode(RuntimeExecutionMode.BATCH);  // STREAMING流 默认， AUTOMATIC自动

        env
            .readTextFile("input/words.txt")
            .flatMap(
                    (String value, Collector<Tuple2<String, Integer>> out) -> {
                        String[] words = value.split(" ");
                        for (String word : words) {
                            out.collect(Tuple2.of(word, 1));
                        }
                    }
            )
            .returns(Types.TUPLE(Types.STRING, Types.INT))
            .keyBy(value -> value.f0)
            .sum(1)
            .print();

        // 3 出发程序执行，Flink是由事件驱动的，只有等到数据到来，才会触发真正的计算，即“延迟执行”或“懒执行”。
        // 显式调用execute()方法，来触发程序执行，等待作业完成后返回一个执行结果(JobExecutionResult)。
        env.execute();  // 第一个就会阻塞
        // env.executeAsync();  // 异步触发，不阻塞。executeAsync()个数 = 生成的flink job数 = Jobmanager中JobMaster数
    }
}
