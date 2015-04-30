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
            System.out.println("creating params: " + inputNum + ", " + keys.size());
            FullConnectionMatrix data = new FullConnectionMatrix(inputNum, (KeyRange) keys);
            data.initWithValue(0.00105f);
//                data.initRandom();
            setLocalCache(data);
        }

        @Override
        public void initOnWorker(int workerIndex, KeyCollection keys) {
            System.out.println("creating update: " + inputNum + ", " + keys.size());
            FullConnectionMatrix data = new FullConnectionMatrix(inputNum, (KeyRange) keys);
            data.initWithValue(0.0f);

            setLocalCache(data);
        }
    }

    private class Results extends DMatrix {

        public Results() {
            super(outputNum);
        }

        @Override
        public void initOnWorker(int workerIndex, KeyCollection keys) {

            System.out.println("creating data on worker: " + inputNum + ", " + keys.size());
            GeneralArray<Float> data = new GeneralArray<Float>(new Float[outputNum]);

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

        registerMatrix(Model.MATRIX_PARAM, new FullConnectionNodes(DMatrix.FLAG_PARAM | DMatrix.FLAG_ON_SERVER));
        registerMatrix(Model.MATRIX_UPDATE, new FullConnectionNodes(DMatrix.FLAG_ON_WORKER));
        registerMatrix(Model.MATRIX_DATA, new Results());
        registerMatrix(Model.MATRIX_ERROR, new Results());
        registerMatrix(Model.MATRIX_DELTA, new Results());
    }


    public void mergeUpdate(Matrix update) {

    }

}
