package com.intel.distml.model.cnn;

import com.intel.distml.util.KeyCollection;
import com.intel.distml.util.Matrix;

/**
 * Created by yunlong on 2/11/15.
 */
public class PoolLayerParam extends Matrix {

    KeyCollection rowKeys, colKeys;

    float[] values;
    int scale;
    int imageNum;

    public PoolLayerParam(int scale, int imageNum) {
        values = new float[scale * scale * imageNum];
        this.scale = scale;
        this.imageNum = imageNum;
    }

    public KeyCollection getRowKeys() {
        return rowKeys;
    }

    public KeyCollection getColKeys() {
        return colKeys;
    }
}
