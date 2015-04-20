package com.intel.distml.model.cnn;

import com.intel.distml.api.DMatrix;
import com.intel.distml.api.Model;
import com.intel.distml.api.neuralnetwork.Layer;
import com.intel.distml.api.neuralnetwork.NeuralNetwork;
import com.intel.distml.util.*;

/**
 * Created by haorx on 15-1-17.
 */
public class FullConnectionLayer extends Layer {

    private class FullConnectionNodes extends DMatrix {

        public FullConnectionNodes(int type) {
            super(type, outputNum);
        }

        @Override
        public void initOnServer(int psIndex, KeyCollection keys) {

            if (type == DMatrix.TYPE_PARAM) {
                System.out.println("creating params: " + inputNum + ", " + keys.size());
                FullConnectionMatrix data = new FullConnectionMatrix(inputNum, (KeyRange) keys);
                data.initWithValue(0.00105f);
//                data.initRandom();
                setLocalCache(data);
            }
        }

        @Override
        public void initOnWorker(int workerIndex, KeyCollection keys) {

            if (type == DMatrix.TYPE_UPDATE) {
                System.out.println("creating update: " + inputNum + ", " + keys.size());
                FullConnectionMatrix data = new FullConnectionMatrix(inputNum, (KeyRange) keys);
                data.initWithValue(0.0f);

                setLocalCache(data);
            }
        }
    }

    private class Results extends DMatrix {

        public Results(int type) {
            super(type, outputNum);
        }

        @Override
        public void initOnWorker(int workerIndex, KeyCollection keys) {

            System.out.println("creating data on worker: " + inputNum + ", " + keys.size());
            Matrix1D<Float>  data = new Matrix1D<Float>(new Float[outputNum]);

            setLocalCache(data);
        }
    }

    int inputNum;
    int outputNum;

    public FullConnectionLayer(NeuralNetwork model, String name, ImageLayer images, int outputNum) {
        super(model, name);

        int inputNum = images.imageNum * images.imageHeight * images.imageWidth;
        System.out.println("inputNum: " + inputNum);

        this.inputNum = inputNum;
        this.outputNum = outputNum;

        FullConnectionEdge edge = new FullConnectionEdge(images, this);
        addEdge(edge);

        registerMatrix(Model.MATRIX_PARAM, new FullConnectionNodes(DMatrix.TYPE_PARAM));
        registerMatrix(Model.MATRIX_UPDATE, new FullConnectionNodes(DMatrix.TYPE_UPDATE));
        registerMatrix(Model.MATRIX_DATA, new Results(DMatrix.TYPE_DATA));
        registerMatrix(Model.MATRIX_ERROR, new Results(DMatrix.TYPE_ERROR));
        registerMatrix(Model.MATRIX_DELTA, new Results(DMatrix.TYPE_DELTA));
    }


    public void mergeUpdate(Matrix update) {

    }

}
