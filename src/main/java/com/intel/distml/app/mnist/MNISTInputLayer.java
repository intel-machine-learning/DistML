package com.intel.distml.app.mnist;

import com.intel.distml.api.DMatrix;
import com.intel.distml.api.Model;
import com.intel.distml.api.neuralnetwork.InputLayer;
import com.intel.distml.api.neuralnetwork.NeuralNetwork;
import com.intel.distml.model.cnn.ImageLayer;
import com.intel.distml.util.Matrix;

/**
 * Created by yunlong on 3/5/15.
 */
public class MNISTInputLayer extends ImageLayer implements InputLayer {

    public MNISTInputLayer(NeuralNetwork model, int w, int h, int n) {
        super(model, "input", w, h, n);

        DMatrix data = new DMatrix(DMatrix.TYPE_DATA, n);
        registerMatrix(Model.MATRIX_DATA, data);
    }

    public void setSample(Matrix sample) {
        getMatrix(Model.MATRIX_DATA).setLocalCache(sample);
    }

}
