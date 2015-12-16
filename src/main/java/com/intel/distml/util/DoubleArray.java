package com.intel.distml.util;

import com.intel.distml.api.DMatrix;
import com.intel.distml.api.Session;
import com.intel.distml.util.KeyCollection;

import java.util.HashMap;

/**
 * Created by yunlong on 12/8/15.
 */
public class DoubleArray extends DMatrix {

    public DoubleArray(long dim) {
        super(dim);
    }

    public HashMap<Long, Double> cache(KeyCollection rows, Session de) {
        HashMap<Long, Double> obj = de.dataBus.fetch(name, rows, KeyCollection.ALL);
        return obj;
    }

    public void pushUpdates(HashMap<Long, Double> updates, Session de) {
        de.dataBus.push(name, updates);
    }

}
