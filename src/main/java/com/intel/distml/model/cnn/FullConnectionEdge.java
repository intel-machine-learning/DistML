package com.intel.distml.model.cnn;

import com.intel.distml.api.Model;
import com.intel.distml.api.databus.DataBus;
import com.intel.distml.api.neuralnetwork.Edge;
import com.intel.distml.api.neuralnetwork.Layer;
import com.intel.distml.api.neuralnetwork.NeuralNetwork;
import com.intel.distml.app.mnist.MNISTModel;
import com.intel.distml.util.*;

/**
 * Created by haorx on 15-1-9.
 */
public class FullConnectionEdge extends Edge {

    public FullConnectionEdge(Layer srcLayer, Layer dstLayer){
        super(srcLayer,dstLayer);
    }

    @Override
    public void computeForward(NeuralNetwork network, int workerIndex, DataBus databus) {
//        System.out.println("==================== Full-Connection forward in Layer:"+dstLayer.index+"====================");

        FullConnectionMatrix matrix = (FullConnectionMatrix) dstLayer.getCache(Model.MATRIX_PARAM);

        ImagesData images = (ImagesData) databus.fetchFromWorker(srcLayer.getGlobalName(Model.MATRIX_DATA));

        Matrix1D<Float> vector = (Matrix1D<Float>) dstLayer.getCache(Model.MATRIX_DATA);

        matrix.calculate(images, vector);
        vector.show();
    }

    /**
     * @param workerIndex
     * @param databus
     */
    @Override
    public void computeBackward(NeuralNetwork network, int workerIndex, DataBus databus) {
        //dstLayer's update and srcLayer's loss
//        System.out.println("======================Full-Connection backward in Layer:" + dstLayer.index + "=================");

        FullConnectionMatrix weights = (FullConnectionMatrix) dstLayer.getCache(Model.MATRIX_PARAM);

        LabeledImage images = (LabeledImage) network.sample;

        Matrix1D<Float> output = (Matrix1D<Float>) dstLayer.getMatrix(Model.MATRIX_DATA).localCache;
        Matrix1D<Float> error = (Matrix1D<Float>) dstLayer.getMatrix(Model.MATRIX_ERROR).localCache;

        Matrix1D<Float> delta=(Matrix1D<Float>) dstLayer.getCache(Model.MATRIX_DELTA);
        FullConnectionMatrix updates = (FullConnectionMatrix) dstLayer.getCache(Model.MATRIX_UPDATE);
        ImagesData sd=(ImagesData) srcLayer.getCache(Model.MATRIX_DATA);
        ImagesData srcDelta=(ImagesData)srcLayer.getCache(Model.MATRIX_DELTA);

        // calculate error
        for (int i = 0; i < error.values.length; i++) {
            error.values[i] =output.element(i)-0;
        }
        error.values[images.label] = output.element(images.label)-1;//TODO:1+?

        // calculate loss
        float loss = 0.0f;//record the loss
        for (int i = 0; i < error.values.length; i++) {
            loss += Math.pow(error.values[i], 2);
        }
        loss = (float) 0.5 * loss / error.values.length;
        System.out.println("loss is"+loss);
        MNISTModel thisModel=((MNISTModel) network);
        if(thisModel.itrIndex==0) {
            thisModel.rL[0] = loss;
            thisModel.itrIndex++;
        }
        else {
            thisModel.rL[thisModel.itrIndex] = (float) 0.001 * loss + (float) 0.99 * thisModel.rL[thisModel.itrIndex - 1];
            System.out.println("accumlate loss is:" + thisModel.rL[thisModel.itrIndex]+"int iterate:"+thisModel.itrIndex);
            thisModel.itrIndex++;
        }

        //calculate output delta
        delta.CopyFrom(output);
        Float f=new Float(1);
        delta.Subtration(f);
        delta.elementwiseMultipy(output);
        delta.elementwiseMultipy(error);

        // calculate update
        float[][] tmpUpdate=new float[updates.values.length][updates.values[0].length];
        computeDstUpdate(delta,sd,tmpUpdate);

        //calculate srcLayer's delta
        //src.delta=dst.delta*dst.param
        computeSrcDelta(delta,weights,srcDelta);

        accumulateUpdate(tmpUpdate,updates.values);
        changeWeight(tmpUpdate,weights.values);

    }
    //Helper Functions
//    public void computeDstUpdate(Matrix1D<Float> delta,ImagesData srcData,FullConnectionMatrix updates,FullConnectionMatrix weights){
//        for(int i=0;i<updates.values.length;i++)
//            for (int j=0;j<updates.values[0].length-1;j++){
//                updates.values[i][j]+= MNISTModel.eta*delta.values[i]*srcData.values[j/16][j%16/4][j%16%4];//plus the previous one
//                weights.values[i][j]-= MNISTModel.eta*delta.values[i]*srcData.values[j/16][j%16/4][j%16%4];
//            }
//
//        for(int i=0;i<updates.values.length;i++) {
//            updates.values[i][updates.values[0].length - 1] += delta.values[i];
//            weights.values[i][weights.values[0].length - 1] -= delta.values[i];
//        }
//        //updates.show();
//    }
    public void computeDstUpdate(Matrix1D<Float> delta,ImagesData srcData,float[][] update){
        for(int i=0;i<update.length;i++)
            for (int j=0;j<update[0].length-1;j++){
                update[i][j]= MNISTModel.eta*delta.values[i]*srcData.values[j/16][j%16/4][j%16%4];
            }

        for(int i=0;i<update.length;i++) {
            update[i][update[0].length - 1] += delta.values[i];
        }
    }
    public void computeSrcDelta(Matrix1D<Float> delta, FullConnectionMatrix param,ImagesData srcDelta){
        for(int i=0;i<srcDelta.values.length;i++)
            for(int j=0;j<srcDelta.values[0].length;j++)
                for(int k=0;k<srcDelta.values[0][0].length;k++) {
                    for(int l=0;l<delta.values.length;l++)
                        srcDelta.values[i][j][k]+=delta.values[l]*param.values[l][i*srcDelta.imageNum+j*srcDelta.imageHeight+srcDelta.imageWidth*k];
                }
    }
    public void accumulateUpdate(float[][] input,Float[][] output){
        for(int i=0;i<input.length;i++)
            for(int j=0;j<input[0].length;j++)
                output[i][j]+=input[i][j];
    }
    public void changeWeight(float[][] input,Float[][] output){
        for(int i=0;i<input.length;i++)
            for(int j=0;j<input[0].length;j++)
                output[i][j]-=input[i][j];
    }
}
