package com.intel.distml.model.akka_tcp;

import com.intel.distml.util.KeyCollection;
import com.intel.distml.util.KeyRange;
import com.intel.distml.util.Matrix;

/**
 * Created by yunlong on 2/4/15.
 */
public class DataItem extends Matrix {

    double x[][];
    int label[];
    int numsample,dim;

    KeyRange rowKeys;

    public DataItem(double[][] x, int[] label, int numsample, int dim) {
        this.x = x;
        this.label = label;
        this.numsample=numsample;
        this.dim=dim;

        rowKeys = new KeyRange(0, x.length-1);
    }


    public KeyCollection getRowKeys() {
        return rowKeys;
    }

    public KeyCollection getColKeys() {
        return KeyRange.Single;
    }

}
