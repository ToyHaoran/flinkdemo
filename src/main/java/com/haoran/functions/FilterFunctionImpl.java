package com.haoran.functions;

import com.haoran.bean.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;

public class FilterFunctionImpl implements FilterFunction<WaterSensor> {

    public String id;

    public FilterFunctionImpl(String id) {
        this.id = id;
    }

    @Override
    public boolean filter(WaterSensor value) throws Exception {
        return this.id.equals(value.getId());
    }
}
