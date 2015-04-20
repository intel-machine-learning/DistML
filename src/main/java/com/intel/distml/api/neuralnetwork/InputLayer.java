package com.intel.distml.api.neuralnetwork;

import com.intel.distml.util.Matrix;

/**
 * Created by yunlong on 3/5/15.
 */
public interface InputLayer {
/*
    public InputLayer(NeuralNetwork network, int dim) {

        super(network, "input");

        DMatrix data = new DMatrix(DMatrix.TYPE_DATA, dim);
////        PartitionInfo sp = new PartitionInfo(PartitionInfo.Type.EXCLUSIVE);
////        sp.exclusiveIndex = 0;
////        data.partition(true, sp);
//        PartitionInfo Wp = new PartitionInfo(PartitionInfo.Type.COPIED);
//        data.partition(false, Wp);

        registerMatrix(Model.MATRIX_DATA, data);
    }

    public void setSample(Matrix sample) {
        getMatrix(Model.MATRIX_DATA).setLocalCache(sample);
    }
*/
    public void setSample(Matrix sample);
}
