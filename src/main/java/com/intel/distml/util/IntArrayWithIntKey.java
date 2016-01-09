package com.intel.distml.util;

/**
 * Created by yunlong on 1/2/16.
 */
public class IntArrayWithIntKey extends SparseArray<Integer, Integer> {

    public IntArrayWithIntKey(long dim) {
        super(dim, DataDesc.KEY_TYPE_INT, DataDesc.ELEMENT_TYPE_INT);
    }

}