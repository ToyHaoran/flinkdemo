package com.haoran.transfrom;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RichFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        SingleOutputStreamOperator<Integer> map = env
                .fromElements(1,2,3,4)
                .map(new RichMapFunction<Integer, Integer>() {
                    @Override  // 每个子任务，在启动时，调用一次
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        System.out.println("索引是：" + getRuntimeContext().getIndexOfThisSubtask()
                                + "，子任务名称是：" + getRuntimeContext().getTaskNameWithSubtasks()
                                + " 的任务的生命周期开始");
                    }

                    @Override
                    public Integer map(Integer integer) throws Exception {
                        return integer + 1;
                    }

                    @Override  // 每个子任务，在结束时，调用一次。如果是flink程序异常挂掉，不会调用close。如果是正常调用cancel命令，可以close。
                    public void close() throws Exception {
                        super.close();
                        System.out.println("索引是：" + getRuntimeContext().getIndexOfThisSubtask() + " 的任务的生命周期结束");
                    }
                });
        map.print();
        env.execute();
    }
}
