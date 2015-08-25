package com.intel.distml.model.rosenblatt;

import com.intel.distml.util.GeneralArray;
import com.intel.distml.util.KeyCollection;

import java.util.Iterator;

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
        Iterator<Long> it = updates.rowKeys.iterator();

        while(it.hasNext()) {
            int k = it.next().intValue();
            setElement(k, element(k) + updates.element(k));
        }
    }

}
