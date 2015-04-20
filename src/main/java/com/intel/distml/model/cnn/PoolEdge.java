package com.intel.distml.model.cnn;

import com.intel.distml.api.Model;
import com.intel.distml.api.databus.DataBus;
import com.intel.distml.api.neuralnetwork.Edge;
import com.intel.distml.api.neuralnetwork.Layer;
import com.intel.distml.api.neuralnetwork.NeuralNetwork;
import com.intel.distml.util.*;

/**
 * Created by haorx on 15-1-16.
 */
public class PoolEdge extends Edge {

    public PoolEdge(Layer srcLayer,Layer dstLayer){
        super(srcLayer,dstLayer);
    }

    @Override
    public void computeForward(NeuralNetwork network, int workerIndex, DataBus dataBus) {

//        System.out.println("====================PoolLayer forward in Layer:" + dstLayer.index + "====================");

        ImagesData images = (ImagesData) dataBus.fetchFromWorker(srcLayer.getGlobalName(Model.MATRIX_DATA));
        ImagesData output = (ImagesData) dstLayer.getMatrix(Model.MATRIX_DATA).localCache;


        ConvAndDownSample(images, output, ((PoolLayer)dstLayer).scale);
//        System.out.println("output in:"+dstLayer.index);output.show();
    }

    @Override
    public void computeBackward(NeuralNetwork network, int workerIndex, DataBus dataBus) {
//        //only one thing:the srcLayer's error;sample layer(dstLayer) is not need to compute error and update
//        System.out.println("=============Pooling Backward in Layer:"+dstLayer.index+";And WorkerIndex:"+workerIndex+"======================");
        ImagesData srcData=(ImagesData)srcLayer.getCache(Model.MATRIX_DATA);
        ImagesData dstDelta= (ImagesData)dstLayer.getCache(Model.MATRIX_DELTA);
        ImagesData srcDelta=(ImagesData)srcLayer.getCache(Model.MATRIX_DELTA);
        //expend dstLayer's error
        float[][][] dstExpendDelta=new float[dstDelta.imageNum][dstDelta.imageHeight*2][dstDelta.imageWidth*2];//TODO:replace magic number
        KroneckerProduct(dstDelta.values,dstExpendDelta,2);
        //compute srcLayer's error
        //src.delta=src.data*(1-src.data)*dstExpendDelta
        computeSrcDelta(dstExpendDelta,srcData.values,srcDelta.values);


    }
//Helper Functions
    public void KroneckerProduct(float[][][] input,float[][][] output,int scale){
        for(int i=0;i<output.length;i++)
            for(int j=0;j<output[0].length;j++)
                for(int k=0;k<output[0][0].length;k++)
                    output[i][j][k]=input[i][j/scale][k/scale];
    }
    public void computeSrcDelta(float[][][] delta,float[][][] srcData,float[][][] srcDelta){
        float[][][] tmp=new float[srcData.length][srcData[0].length][srcData[0][0].length];
        CNNUtil.copyFrom(srcData,tmp);
        CNNUtil.Subtration(1.0f, tmp);
        CNNUtil.elementwiseMultipy(delta, tmp);
        CNNUtil.elementwiseMultipy(srcData,tmp);
        CNNUtil.Multiply(0.25f,tmp);
        CNNUtil.copyFrom(tmp,srcDelta);
    }
    public void ConvAndDownSample(ImagesData images, ImagesData output, int scale) {

        KeyRange rowKeys = (KeyRange) output.getRowKeys();

        for (long i = rowKeys.firstKey; i <= rowKeys.lastKey; i++) {
            float[][] image = images.element(i);
            float[][] outImage = output.element(i);
            float[][] tmp=new float[image.length-2+1][image[0].length-2+1];
            convSample(image,tmp);
            downSample(tmp, scale, outImage);
        }
    }

    public void convSample(float[][] input,float[][] output){
        int height=input.length;
        int width=input[0].length;
        int kernelH=2,kernelW=2;
        CNNUtil.convolution2D(input,width,height,((PoolLayer)dstLayer).sampleKernel,kernelW,kernelH,output);
    }
    public float[][] downSample(float[][] input,int scale, float[][] output) {

        for (int i = 0; i < output.length; i++) {
            for (int j = 0; j < output[0].length; j++) {
                output[i][j] = input[i * scale][j * scale];
            }
        }
        return output;
    }

}
