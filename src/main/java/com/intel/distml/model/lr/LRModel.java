package com.intel.distml.model.lr;

import com.intel.distml.api.DMatrix;
import com.intel.distml.api.DParam;
import com.intel.distml.api.databus.DataBus;
import com.intel.distml.api.Model;
import com.intel.distml.api.neuralnetwork.DUpdate;
import com.intel.distml.model.rosenblatt.Weights;
import com.intel.distml.util.KeyCollection;
import com.intel.distml.util.Matrix;

import java.util.List;

/**
 * Created by lq on 3/12/15.
 */
public class LRModel extends Model {
    int dim;

    private class SensorNodes extends DParam {

        public SensorNodes() {
            super(dim);
        }

        @Override
        public void initOnServer(int psIndex, KeyCollection keys) {

            Double w[] = new Double[dim];
            for (int i = 0; i < dim; i++) {
                w[i] = 0.0;//((double)(Math.random()-.5));
            }
            setLocalCache(new Weights(w));
        }

        public void mergeUpdate(int serverIndex, Matrix update) {
            ((Weights) localCache).mergeUpdates((Weights) update);
        }
    }

    private class UpdateMatrix extends DUpdate {

        public UpdateMatrix() {
            super(dim);
        }

        @Override
        public void initOnWorker(int psIndex, KeyCollection keys) {
            Double w[] = new Double[dim];
            for (int i = 0; i < dim; i++) {
                w[i] = 0.0;//((double)(Math.random()-.5));
            }
            setLocalCache(new Weights(w));
        }
    }

    public LRModel(int d/*int psNum, int workerGroupSize */) {

//        this.psNum = psNum;
//        this.workerGroupSize = workerGroupSize;
//
        super();
        dim = d;
        System.out.println("I am initializing");
        registerMatrix("weights", new SensorNodes());
        registerMatrix("momentum", new UpdateMatrix());
    }

    public void show() {
        ((Weights) getMatrix("weights").localCache).show();
    }

    @Override
    public void compute(Matrix sample, int workerIndex, DataBus dataBus, int iter) {
        // sample.data 100*2
        // sample.label 100*1
        //

        Weights weights = (Weights) getMatrix("weights").localCache;
        Weights momentum = (Weights) getMatrix("momentum").localCache;
        DataItem samples = (DataItem) sample;
//        for (int i = 0; i < samples.numsample; i++) {
//            for (int j = 0; j < samples.dim; j++) {
//                System.out.print(samples.x[i][j] + " ");
//            }
//            System.out.println(samples.label[i]);
//        }
        Double w[] = new Double[samples.dim];
        Double w2[] = new Double[samples.dim];
        Weights grad = new Weights(w);

        for (int j = 0; j < samples.dim; j++) {
            w2[j] = weights.values[j];
        }
        double lr = 0.01;
        //    double cost=costfunction(weights,samples,grad);
//        for ( int i=0;i<samples.dim;i++){
//            System.out.println(grad.values[i]);
//        }
            double cost = costfunction(weights, samples, grad);
            for (int j = 0; j < samples.dim; j++) {
                //    System.out.println(momentum.values[j]);
                momentum.values[j] = 0.9 * momentum.values[j] + 0.1 * grad.values[j];
                weights.values[j] -= lr * momentum.values[j];

                // weights.values[j] -= lr * grad.values[j];
            }
            System.out.println(cost);


        for (int j = 0; j < samples.dim; j++) {
            momentum.values[j] = weights.values[j] - w2[j];
        }

        dataBus.pushUpdate("weights", momentum);
        System.out.println("accuracy is  " + predict(weights, samples));


    }


    private double predict(Weights theta, Matrix sample) {
        int accuracy = 0;
        DataItem samples = (DataItem) sample;
        Double tmp[] = new Double[samples.numsample];

        // W * x
        for (int i = 0; i < samples.numsample; i++) {
            tmp[i] = 0.0;
            for (int j = 0; j < samples.dim; j++) {
                tmp[i] += samples.x[i][j] * theta.values[j];
            }
            tmp[i] = sigmoid(tmp[i]);
            if (tmp[i] <= 0.5)
                tmp[i] = 0.0;
            else
                tmp[i] = 1.0;
            if (tmp[i] == samples.label[i])
                accuracy += 1;
        }
        return (double) accuracy / (double) samples.numsample;


    }

    public double costfunction(Weights theta, Matrix sample, Weights grad) {
        // theta is     sample.dim * 1
        //sample.x is  sample.numsample * sample.dim
        // return value: grad is      sample.dim * 1
        DataItem samples = (DataItem) sample;
        Double tmp[] = new Double[samples.numsample];

        // W * x
        for (int i = 0; i < samples.numsample; i++) {
            tmp[i] = 0.0;
            for (int j = 0; j < samples.dim; j++) {
                tmp[i] += samples.x[i][j] * theta.values[j];
            }
            tmp[i] = sigmoid(tmp[i]);

        }
        double cost = 0;
        for (int i = 0; i < samples.numsample; i++) {
            cost -= samples.label[i] * Math.log(tmp[i]);
            cost -= (1 - samples.label[i]) * Math.log(1 - tmp[i]);
        }
        for (int i = 0; i < samples.numsample; i++) {
            tmp[i] -= samples.label[i];
        }
        for (int j = 0; j < samples.dim; j++) {
            grad.values[j] = 0.0;
            for (int i = 0; i < samples.numsample; i++) {
                grad.values[j] += samples.x[i][j] * tmp[i];
            }
            grad.values[j] /= samples.numsample;
        }


        return cost / samples.numsample;
    }


    private double sigmoid(double val) {
        return (1.0 / (1.0 + Math.exp(-val)));
    }

    /**
     * If applications uses non-matrix sample, this method should be override
     * to make it usable.
     * <p/>
     * todo: move this method out of api.
     *
     * @param samples
     * @return
     */
    public Matrix transformSamples(List<Object> samples) {
        //     String str = (String) samples.get(0);
        //    System.out.println("sample string: [" + str + "]");
        // String[] strs = str.split(" ");
        int numsample = samples.size();
        double[][] x = new double[numsample][dim];
        int[] label = new int[numsample];
        for (int i = 0; i < numsample; i++) {
            String str = (String) samples.get(i);
            //   System.out.println("sample string: [" + str + "]");
            String[] tmp = str.split(",");
            x[i][0] = 1.0;
            x[i][1] = Double.valueOf(tmp[0]);
            x[i][2] = Double.valueOf(tmp[1]);
            label[i] = Integer.valueOf(tmp[2]);
        }
        DataItem data = new DataItem(x, label, numsample, dim);
        return data;

    }
//    public Matrix transformSamples(List<Object> samples) {
//        String str = (String)samples.get(0);
//        System.out.println("sample string: [" + str + "]");
//        String[] strs = str.split(" ");
//        double x[][];
//        int label[];
//        DataItem data=new DataItem(x,label);
////        int INPUT_IMAGE_WIDTH=28;
////        int INPUT_IMAGE_HEIGHT=28;
////        LabeledImage data = new LabeledImage(INPUT_IMAGE_WIDTH, INPUT_IMAGE_HEIGHT);
////        float[][] img = data.element(0);
////        for (int i = 0; i < INPUT_IMAGE_WIDTH; i++) {
////            for (int j = 0; j < INPUT_IMAGE_HEIGHT; j++) {
////                img[i][j] = Integer.parseInt(strs[i*INPUT_IMAGE_WIDTH+j]) * 1.0f / 255;
////            }
////        }
////
////        data.label = Integer.parseInt(strs[INPUT_IMAGE_WIDTH*INPUT_IMAGE_HEIGHT]);
////
////        return data;
//    }
}
