package com.intel.distml.model.sparselr;

import com.intel.distml.util.HashMapMatrix;

import java.util.HashMap;

/**
 * Created by yunlong on 2/3/15.
 */
public class LRSample extends HashMapMatrix<Double> {

    public double label;


    public LRSample() {
        this(new HashMap<Long, Double>(), 0.0);
    }

    public LRSample(HashMap<Long, Double> d, double l) {
        super(d);
        label = l;
    }

    public HashMap<Long, Double> data() {
        return data;
    }

    public double timesBy(HashMapMatrix<WeightItem> weights) {
        double sumation = 0.0;

        for (long key : data.keySet()) {
            if (weights.data.containsKey(key)) {
                sumation += data.get(key) * weights.data.get(key).value;
            }
        }
        return sumation;
    }

    public void updateWeights(double eta, double error, SparseWeights weights) {

        for (long key : data.keySet()) {
            double bias = eta * error * data.get(key);
            weights.update(key, bias);
        }
    }
}
