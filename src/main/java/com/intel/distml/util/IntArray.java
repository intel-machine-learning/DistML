package com.intel.distml.util;

import com.intel.distml.api.DMatrix;
import com.intel.distml.api.Session;
import com.intel.distml.util.KeyCollection;

import java.util.HashMap;

/**
 * Created by yunlong on 12/8/15.
 */
public class IntArray extends DMatrix {

    public IntArray(long dim) {
        super(dim);
    }

    public HashMap<Long, Integer> cache(KeyCollection rows, Session de) {
        HashMap<Long, Integer> obj = de.dataBus.fetch(name, rows, KeyCollection.ALL);
        return obj;
    }

    public void pushUpdates(HashMap<Long, Integer> updates, Session de) {
        de.dataBus.push(name, updates);
    }

    public void pushUpdates(Integer[] updates, Session de) {
        de.dataBus.push(name, updates);
    }
}
