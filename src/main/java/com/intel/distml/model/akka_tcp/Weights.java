package com.intel.distml.model.akka_tcp;

import com.intel.distml.util.Matrix1D;

/**
 * Created by yunlong on 2/4/15.
 */
public class Weights extends Matrix1D<Double> {

    public Weights() {
    }

    public Weights(Double[] w) {
        super(w);
    }

    public void mergeUpdates(Weights updates) {
        for (int i = 0; i < dim; i++) {
            values[i] += updates.values[i];
        }
    }

}
