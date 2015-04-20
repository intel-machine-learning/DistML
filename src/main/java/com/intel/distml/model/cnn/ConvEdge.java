package com.intel.distml.model.cnn;

import com.intel.distml.api.Model;
import com.intel.distml.api.databus.DataBus;
import com.intel.distml.api.neuralnetwork.Edge;
import com.intel.distml.api.neuralnetwork.Layer;
import com.intel.distml.api.neuralnetwork.NeuralNetwork;
import com.intel.distml.util.*;

/**
 * Created by haorx on 15-1-9.
 */
public class ConvEdge extends Edge {

    public ConvEdge(Layer srcLayer, Layer dstLayer){
        super(srcLayer,dstLayer);
    }

    @Override
    public void computeForward(NeuralNetwork network, int workerIndex, DataBus databus) {
//        System.out.println("====================Convolution forward in Layer:"+dstLayer.index+"====================");

        ConvKernels kernels = (ConvKernels) dstLayer.getCache(Model.MATRIX_PARAM);

        ImagesData images = (ImagesData) network.sample;
        if (srcLayer != null) {
            images = (ImagesData) databus.fetchFromWorker(srcLayer.getGlobalName(Model.MATRIX_DATA));
        }


        ImagesData output = (ImagesData) dstLayer.getCache(Model.MATRIX_DATA);

        kernels.conv(images, output);
        output.sigmoid();
//        System.out.println("output in:"+dstLayer.index);output.show();
    }

    /**
     * @param workerIndex
     * @param databus
     */
    @Override
    public void computeBackward(NeuralNetwork network, int workerIndex, DataBus databus) {
        //dstLayer's update and srcLayer's loss
//        System.out.println("======================Convolution backward in Layer:"+dstLayer.index+"=================");
        ImagesData srcData = (ImagesData)srcLayer.getCache(Model.MATRIX_DATA);
        ConvKernels update =(ConvKernels) dstLayer.getCache(Model.MATRIX_UPDATE);
        ConvKernels param =(ConvKernels) dstLayer.getCache(Model.MATRIX_PARAM);
        ImagesData delta = (ImagesData)dstLayer.getCache(Model.MATRIX_DELTA);
        ConvKernels tmpUpdate=new ConvKernels(update.kernalWidth,update.kernalHeight,update.inputCount, (KeyRange)update.rowKeys);

        //compute dstLayer's update
        // dstLayer.update = dstError*srcData
        computeDstUpdate(srcData,delta,tmpUpdate);
        if(srcLayer.index==0){
            accumulateUpdate(tmpUpdate,update);
            changeParam(tmpUpdate,param);
            System.out.println("=========completed one sample============");
            return;
        }
        ImagesData srcDelta=(ImagesData)srcLayer.getCache(Model.MATRIX_DELTA);
        //compute srcLayer's delta
        // srcLayer.delta = dstDelta*kernels
        computeSrcDelta(delta, param, srcDelta);

        accumulateUpdate(tmpUpdate,update);
        changeParam(tmpUpdate,param);
    }
    //Help Functions
    public void computeDstUpdate(ImagesData srcdata,ImagesData dstdelta,ConvKernels output){
        int inputHeight=srcdata.values[0].length;
        int inputWidth=srcdata.values[0][0].length;
        int KernelHeight=dstdelta.values[0].length;
        int KernelWidth=dstdelta.values[0][0].length;
        for(int i=0;i<dstdelta.values.length;i++)
            for(int j=0;j<srcdata.values.length;j++)
                CNNUtil.convolution2D(srcdata.values[j],inputWidth,inputHeight,dstdelta.values[i],KernelWidth,KernelHeight,output.values[i][j]);
    }
    public void computeSrcDelta(ImagesData dstDelta,ConvKernels param,ImagesData ouput){
        float[][] tmpOut=new float[ouput.imageWidth][ouput.imageHeight];
        for(int i=0;i<ouput.imageNum;i++)
            for(int j=0;j<dstDelta.imageNum;j++){
            float[][] tmp = new float[dstDelta.imageWidth + 8][dstDelta.imageHeight + 8];
                CNNUtil.expendEdge(dstDelta.values[i],tmp,8/2);
                int inputHeight=tmp.length;
                int inputWidth=tmp[0].length;
                int KernelHeight=param.kernalHeight;
                int KernelWidth=param.kernalWidth;
                CNNUtil.convolution2D(tmp,inputWidth,inputHeight,param.values[j][i],KernelWidth,KernelHeight,tmpOut);
                CNNUtil.elementwiseAddition(ouput.values[i],tmpOut);

        }
    }

    public void changeParam(ConvKernels update,ConvKernels param){
        for(int i=0;i<param.outputCount;i++)
            for(int j=0;j<param.inputCount;j++)
                for(int k=0;k<param.kernalHeight;k++)
                    for(int l=0;l<param.kernalWidth;l++)
                        param.values[i][j][k][l]-=update.values[i][j][k][l];
    }
    public void accumulateUpdate(ConvKernels thisUpdate,ConvKernels update){
        for(int i=0;i<update.outputCount;i++)
            for(int j=0;j<update.inputCount;j++)
                for(int k=0;k<update.kernalHeight;k++)
                    for(int l=0;l<update.kernalWidth;l++)
                        update.values[i][j][k][l]+=thisUpdate.values[i][j][k][l];
    }
}
