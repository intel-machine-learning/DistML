package com.intel.distml.model.rosenblatt;

import com.intel.distml.api.DMatrix;
import com.intel.distml.api.DParam;
import com.intel.distml.api.databus.DataBus;
import com.intel.distml.api.Model;
import com.intel.distml.model.cnn.ConvKernels;
import com.intel.distml.util.KeyCollection;
import com.intel.distml.util.Logger;
import com.intel.distml.util.Matrix;

/**
 * Created by yunlong on 2/4/15.
 */
public class Rosenblatt extends Model {

    public class SensorNodes extends DParam {

        public SensorNodes() {
            super(dim+1);
        }

        @Override
        public void initOnServer(int psIndex, KeyCollection keys) {

            int dim = (int)keys.size();
            Double w[] = new Double[dim + 1];
            for (int i = 0; i <= dim; i++){
                w[i] = ((double)(Math.random()-.5));
            }

            setLocalCache(new Weights(w));
        }

        public void mergeUpdate(int serverIndex, Matrix update) {
            ((ConvKernels) localCache).mergeUpdate((ConvKernels)update);
        }
    }

    public static final int THRESHHOLD = 0;
    public static final double learning_rate = 0.01;
    int dim;

    public Rosenblatt(int dim) {
        registerMatrix(Model.MATRIX_PARAM, new SensorNodes());
    }

    public void showResult() {

    }

    public Matrix transformSample(Object sample) {
        return (Matrix) sample;
    }

    @Override
    public void compute(Matrix s, int workerIndex, DataBus dataBus, int iter) {

        Weights weights = (Weights) getMatrix(MATRIX_PARAM).localCache;

        PointSample sample = (PointSample) s;

        double sum = 0.0;
        for (int i = 0; i < dim; i++) {
            Logger.DebugLog("x[" + i + "]" + " = " + sample.x[i], Logger.Role.APP, 0);
            sum += sample.x[i] * weights.values[i];
        }
        sum += weights.values[dim];

        int activation = (sum >= THRESHHOLD) ? 1 : -1;
        double err = sample.label - activation;
        System.out.println("=========error: " + err + " ============");

        for(int i = 0; i < dim; i++){
            double a = sample.x[i];
            double adjustment = learning_rate * sample.x[i] * err;
            weights.values[i] = weights.values[i] + adjustment;
        }
        double adjustment = learning_rate * err;
        weights.values[dim] = weights.values[dim] + adjustment;
    }
}
