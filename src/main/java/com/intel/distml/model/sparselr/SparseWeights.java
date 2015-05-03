package com.intel.distml.model.sparselr;

import com.intel.distml.util.HashMapMatrix;
import com.intel.distml.util.Matrix;

import java.util.HashMap;

/**
 * Created by yunlong on 2/20/15.
 */
public class SparseWeights extends HashMapMatrix<WeightItem> {

    public SparseWeights() {
        super();
    }

    public SparseWeights(int initialCapacity) {
        super(initialCapacity);
    }

    public SparseWeights(HashMap<Long, WeightItem> data) {
        super(data);
    }

    protected Matrix createEmptySubMatrix() {
        SparseWeights weights = new SparseWeights();
        return weights;
    }

    @Override
    public String toString() {
        return "(SparseWeights: size=" + data.size() + ")";
    }

    public void timesBy(double factor) {
        for (WeightItem item : data.values()) {
            item.value *= factor;
        }
    }

    public void update(long key, double update) {
        if (data.containsKey(key)) {
            WeightItem item = data.get(key);
            item.value += update;
            item.count += 1;
        }
        else {
            data.put(key, new WeightItem(update, 1));
        }
    }

    public boolean mergeUpdate(Matrix m) {
        System.out.println("merge begin");

        HashMapMatrix<WeightItem> items = (HashMapMatrix<WeightItem>) m;
        int newCount = 0;
        int oldCount = 0;
        for (long key : items.data.keySet()) {
            if (data.containsKey(key)) {
                WeightItem oldItem = data.get(key);
                WeightItem newItem = items.data.get(key);
                int total = oldItem.count + newItem.count;
                oldItem.value = (oldItem.value * oldItem.count + newItem.value * newItem.count) / total;
                oldItem.count = total;
                oldCount++;
            }
            else {
                data.put(key, items.data.get(key));
                newCount++;
            }
        }

        System.out.println("merge done, size=" + data.size() + ", new=" + newCount + ", old=" + oldCount);
        return true;
    }
}
