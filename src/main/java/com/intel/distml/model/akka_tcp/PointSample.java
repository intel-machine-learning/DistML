package com.intel.distml.model.akka_tcp;

import com.intel.distml.util.KeyCollection;
import com.intel.distml.util.KeyRange;
import com.intel.distml.util.Matrix;

/**
 * Created by yunlong on 2/4/15.
 */
public class PointSample extends Matrix {

    double x[];
    int label;

    KeyRange rowKeys;

    public PointSample(double[] x, int label) {
        this.x = x;
        this.label = label;

        rowKeys = new KeyRange(0, x.length-1);
    }


    public KeyCollection getRowKeys() {
        return rowKeys;
    }

    public KeyCollection getColKeys() {
        return KeyRange.Single;
    }

}
