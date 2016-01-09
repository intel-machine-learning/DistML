package com.intel.distml.util;

import com.intel.distml.api.DMatrix;
import com.intel.distml.api.Session;

import java.util.HashMap;

/**
 * Created by jimmy on 15-12-29.
 */
public class DoubleMatrix extends SparseMatrix<Long, Double> {
    public DoubleMatrix(long rows, int cols) {
        super(rows, cols, DataDesc.KEY_TYPE_LONG, DataDesc.ELEMENT_TYPE_DOUBLE);
    }

    protected boolean isZero(Double value) {
        return value == 0.0;
    }
    protected Double[] createValueArray(int size) {
        return new Double [size];
    }

}
