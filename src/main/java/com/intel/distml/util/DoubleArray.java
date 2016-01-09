package com.intel.distml.util;

import com.intel.distml.api.DMatrix;
import com.intel.distml.api.Session;
import com.intel.distml.util.KeyCollection;

import java.util.HashMap;

/**
 * Created by yunlong on 12/8/15.
 */

public class DoubleArray extends SparseArray<Long, Double> {

    public DoubleArray(long dim) {
        super(dim, DataDesc.KEY_TYPE_LONG, DataDesc.ELEMENT_TYPE_DOUBLE);
    }

}