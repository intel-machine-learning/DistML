package com.intel.distml.model.sparselr;

import com.intel.distml.api.*;
import com.intel.distml.api.databus.DataBus;
import com.intel.distml.api.databus.MonitorDataBus;
import com.intel.distml.util.*;

import java.util.List;

/**
 * Created by yunlong on 1/28/15.
 */
public class SparseLRModel extends Model {

    private class WeightNodes extends DParam {

        public WeightNodes() {
            super(dim);
        }

        @Override
        public void initOnServer(int psIndex, KeyCollection keys) {
            setLocalCache(new SparseWeights());
        }

        @Override
        public void mergeUpdate(int serverIndex, Matrix update) {
            ((SparseWeights) localCache).mergeUpdate(update);
        }
    }

    int dim;
    double initialLearningRate = 0.001;
    double minLearningRate = 0.00001;
    double learningRate = initialLearningRate;


    public SparseLRModel(int dim) {
        this.dim = dim;
        autoFetchParams = false;
        autoPushUpdates = false;

        DMatrix weights = new WeightNodes();
        weights.setPartitionStrategy(DMatrix.PARTITION_STRATEGY_HASH);
        registerMatrix(Model.MATRIX_PARAM, weights);
    }

    @Override
    public Matrix transformSamples(List<Object> samples) {

        LRSample[] array = new LRSample[samples.size()];
        for (int i = 0; i < samples.size(); i++) {
            array[i] = (LRSample) samples.get(i);
        }

        GeneralArray<LRSample> matrix =  new GeneralArray<LRSample>(array);
        return matrix;
    }

    private void prefetch(GeneralArray<LRSample> samples, DataBus dataBus) {

        KeyList keyList = new KeyList();

        for(LRSample sample : samples.values) {
            for (long key : sample.data.keySet()) {
                keyList.addKey(key);
            }
        }

        setCache(Model.MATRIX_PARAM, dataBus.fetchFromServer(Model.MATRIX_PARAM, keyList));
    }

    @Override
    public void compute(Matrix s, int workerIndex, DataBus dataBus) {
//        double eta=0.001;
//        double Maxiter=10000.0;
//        for (double iter = 0.0;iter< Maxiter;iter+=1.0){
//
            GeneralArray<LRSample> samples = (GeneralArray<LRSample>) s;
//            prefetch(samples, dataBus);

        DMatrix params = getMatrix(Model.MATRIX_PARAM);
//        System.out.println("param partitions: " + params.serverPartitions());
            SparseWeights weights = (SparseWeights) params.localCache;

            for (int i = 0; i < samples.values.length; i++) {
                LRSample sample = samples.values[i];
                computeOneSample(sample, weights, learningRate);
            }

//            dataBus.pushUpdate(Model.MATRIX_PARAM, getCache(Model.MATRIX_PARAM));
//            System.out.println("@@@@@@@@@@@@@@@  Inner iter:"+ iter+" Now accuracy is  " + predict(s, workerIndex, dataBus));
//        }
    }

    public void preTraining(int workerIndex, DataBus dataBus) {
        setCache(Model.MATRIX_PARAM, new SparseWeights());
    }

    public void postTraining(int workerIndex, DataBus dataBus) {

        Matrix weights = getCache(Model.MATRIX_PARAM);
        HashMapMatrix.KeySet keys = (HashMapMatrix.KeySet) weights.getRowKeys();
        KeyCollection[] sets = keys.split(200);
        for (KeyCollection set : sets) {
            System.out.println("push weights: " + set.size());
            dataBus.pushUpdate(Model.MATRIX_PARAM, weights.subMatrix(set, KeyCollection.ALL));
            System.out.println("push done");
        }
    }

    public void progress(long totalSamples, long progress, MonitorDataBus dataBus) {
        learningRate = initialLearningRate * (1.0 - progress/totalSamples);
        if (learningRate < minLearningRate) {
            learningRate = minLearningRate;
        }

        dataBus.broadcast("learningRate", learningRate);
    }

    public void variableChanged(String name, Object value) {
        learningRate = ((Double)value).doubleValue();
    }

    public double predict(Matrix s, int workerIndex, DataBus dataBus) {

        GeneralArray<LRSample> samples = (GeneralArray<LRSample>) s;
        prefetch(samples, dataBus);

        SparseWeights  weights = (SparseWeights) getMatrix(Model.MATRIX_PARAM).localCache;
        int accuracy=0;
        for (int i = 0; i < samples.values.length; i++) {
            LRSample sample = samples.values[i];
            double weightSum = sample.timesBy(weights);
            double predictValue = 1 / (1 + Math.exp(-weightSum));
            if (predictValue<= 0.5)
                predictValue = 0.0;
            else
                predictValue = 1.0;
            if (predictValue == sample.label)
                accuracy += 1;
        }

        return (double) accuracy / (double) samples.values.length;

    }

    public void computeOneSample(LRSample sample, SparseWeights weights, double eta) {

        double weightSum = sample.timesBy(weights);
        double predictValue = 1 / (1 + Math.exp(-weightSum));

        double error = sample.label - predictValue;

        sample.updateWeights(eta, error, weights);
    }
}
