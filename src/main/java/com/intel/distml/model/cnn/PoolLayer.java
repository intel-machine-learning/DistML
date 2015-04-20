package com.intel.distml.model.cnn;

import com.intel.distml.api.DMatrix;
import com.intel.distml.api.Model;
import com.intel.distml.api.neuralnetwork.NeuralNetwork;

public class PoolLayer extends ImageLayer {

    ImageLayer input;
    public int scale;   // the only parameter for this layer
    public float[][] sampleKernel;

    public PoolLayer(NeuralNetwork model, String name, ImageLayer images, int scale) {
        super(model, name, images.imageWidth/scale, images.imageHeight/scale, images.imageNum);

        System.out.println("pool layer: imageNum: " + imageNum);

        this.scale = scale;
        this.input = images;

        this.sampleKernel=new float[2][2];
        for(int i=0;i<2;i++)
            for(int j=0;j<2;j++)
                sampleKernel[i][j]=1.0f/(scale*scale);

        PoolEdge edge = new PoolEdge(images, this);
        addEdge(edge);

        registerMatrix(Model.MATRIX_ERROR, new ImageNodes(DMatrix.TYPE_ERROR));
        registerMatrix(Model.MATRIX_DATA, new ImageNodes(DMatrix.TYPE_DATA));
        registerMatrix(Model.MATRIX_DELTA, new ImageNodes(DMatrix.TYPE_DELTA));
    }


}
