package com.intel.distml.util;

/**
 * Created by yunlong on 12/8/15.
 */
public class IntMatrixWithIntKey extends SparseMatrix<Integer, Integer> {

    public IntMatrixWithIntKey(long rows, int cols) {
        super(rows, cols, DataDesc.KEY_TYPE_INT, DataDesc.ELEMENT_TYPE_INT);
    }

    protected boolean isZero(Integer value) {
        return value == 0;
    }

    protected Integer[] createValueArray(int size) {
        return new Integer[size];
    }


}
