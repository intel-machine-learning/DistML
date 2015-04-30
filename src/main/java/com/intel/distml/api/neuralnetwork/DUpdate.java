package com.intel.distml.api.neuralnetwork;

import com.intel.distml.api.DMatrix;
import com.intel.distml.util.Matrix;

/**
 * Created by yunlong on 3/28/15.
 */
public class DUpdate extends DMatrix {

    public DUpdate(int rows) {
        super(DMatrix.FLAG_UPDATE | DMatrix.FLAG_ON_WORKER, rows);
    }

}
