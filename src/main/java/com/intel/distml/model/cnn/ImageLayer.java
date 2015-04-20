package com.intel.distml.model.cnn;

import com.intel.distml.api.DMatrix;
import com.intel.distml.api.neuralnetwork.Layer;
import com.intel.distml.api.neuralnetwork.NeuralNetwork;
import com.intel.distml.util.KeyCollection;

/**
 * Created by yunlong on 2/8/15.
 */
public class ImageLayer extends Layer {

    protected class ImageNodes extends DMatrix {

        public ImageNodes(int type) {
            super(type, imageNum);
        }

        @Override
        public void initOnWorker(int workerIndex, KeyCollection keys) {
            ImagesData data = new ImagesData(imageWidth, imageHeight, keys);
            setLocalCache(data);
        }
    }

    protected int imageWidth;
    protected int imageHeight;

    protected int imageNum;

    public ImageLayer(NeuralNetwork model, String name, int w, int h, int n) {

        super(model, name);

        this.imageWidth = w;
        this.imageHeight = h;

        this.imageNum = n;
    }


}
