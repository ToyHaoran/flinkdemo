package com.haoran.watermark;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

public class MyPuntuatedWatermarkGenerator<T> implements WatermarkGenerator<T> {
    private long delayTs;  // 乱序等待时间
    private long maxTs;  // 用来保存 当前为止 最大的事件时间

    public MyPuntuatedWatermarkGenerator(long delayTs) {
        this.delayTs = delayTs;
        this.maxTs = Long.MIN_VALUE + this.delayTs + 1;
    }

    // 每条数据来，都会调用一次： 用来提取最大的事件时间，保存下来,并发射watermark
    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        maxTs = Math.max(maxTs, eventTimestamp);
        output.emitWatermark(new Watermark(maxTs - delayTs - 1));
        System.out.println("调用onEvent方法，获取目前为止的最大时间戳=" + maxTs+",watermark="+(maxTs - delayTs - 1));
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {}
}
