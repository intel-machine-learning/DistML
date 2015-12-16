package com.intel.distml.util;

import com.intel.distml.api.DMatrix;
import com.intel.distml.api.Session;
import com.intel.distml.util.KeyCollection;

import java.util.HashMap;

/**
 * Created by yunlong on 12/8/15.
 */
public class IntMatrix extends DMatrix {

    public int cols;

    public IntMatrix(long rows, int cols) {
        super(rows);

        this.cols = cols;
    }

    public HashMap<Long, Integer[]> cache(KeyCollection rows, Session de) {
        HashMap<Long,Integer[]> obj = de.dataBus.fetch(name, rows, KeyCollection.ALL);
        return obj;
    }

    public void pushUpdates(HashMap<Long, int[]> updates, Session de) {
        de.dataBus.push(name, updates);
    }

}
