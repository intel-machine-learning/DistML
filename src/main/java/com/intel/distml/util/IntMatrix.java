package com.intel.distml.util;

import com.intel.distml.api.DMatrix;
import com.intel.distml.api.Session;
import com.intel.distml.util.KeyCollection;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yunlong on 12/8/15.
 */
public class IntMatrix extends SparseMatrix<Long, Integer> {

    public IntMatrix(long rows, int cols) {
        super(rows, cols, DataDesc.KEY_TYPE_LONG, DataDesc.ELEMENT_TYPE_INT);
    }

    protected boolean isZero(Integer value) {
        return value == 0;
    }

    protected Integer[] createValueArray(int size) {
        return new Integer[size];
    }

}
