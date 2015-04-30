package com.intel.distml.model.rosenblatt;

import com.intel.distml.util.GeneralArray;

/**
 * Created by yunlong on 2/4/15.
 */
public class Weights extends GeneralArray<Double> {

    public Weights() {
    }

    public Weights(Double[] w) {
        super(w);
    }

    public void mergeUpdates(Weights updates) {
        for (int i = 0; i < rowKeys.size(); i++) {
            values[i] += updates.values[i];
        }
    }

}
