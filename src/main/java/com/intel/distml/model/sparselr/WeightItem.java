package com.intel.distml.model.sparselr;

import java.io.Serializable;

/**
 * Created by yunlong on 2/12/15.
 */
public class WeightItem implements Serializable {

    public double value;
    public int count;

    public WeightItem(WeightItem _item) {
        value = _item.value;
        count = _item.count;
    }

    public WeightItem(double _value, int _count) {
        this.value = value;
        this.count = _count;
    }

    public WeightItem plus(WeightItem _item) {
        this.value += _item.value;
        this.count += _item.count;

        return this;
    }
}
