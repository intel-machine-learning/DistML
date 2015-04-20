package com.intel.distml.util;

import org.tukaani.xz.UnsupportedOptionsException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Created by yunlong on 2/1/15.
 */
public class Long2DoubleHashMap extends Matrix {

    protected HashMap<Long, Double> data;
    protected long dim;

    private class KeySet implements KeyCollection {

        public boolean contains(long key) {
            return data.containsKey(key);
        }

        public KeyCollection intersect(KeyCollection otherKeys) {
            if (otherKeys == KeyCollection.ALL) {
                return this;
            }

            Set<Long> keys = data.keySet();
            KeyList result = new KeyList();

            if (otherKeys.size() < keys.size()) {
                Iterator<Long> it = otherKeys.iterator();
                while (it.hasNext()) {
                    long k = it.next();
                    if (keys.contains(k))
                        result.addKey(k);
                }
            } else {
                Iterator<Long> it = keys.iterator();
                while (it.hasNext()) {
                    long k = it.next();
                    if (otherKeys.contains(k))
                        result.addKey(k);
                }
            }

            return result;
        }

        public Iterator<Long> iterator() {
            Set<Long> keys = data.keySet();
            return keys.iterator();
        }

        public boolean isEmpty() {
            return data.isEmpty();
        }

        public int size() {
            return data.size();
        }
/*
        public PartitionInfo partitionEqually(int hostNum) {
            throw new UnsupportedOperationException("This class is internally used only.");
        }

        @Override
        public KeyCollection[] split(int hostNum) {
            throw new UnsupportedOperationException("This class is internally used only.");
        }
*/
    }

    public Long2DoubleHashMap(long dim) {
        data = new HashMap<Long, Double>();
        this.dim = dim;
    }

    public Long2DoubleHashMap(int dim, HashMap<Long, Double> data) {
        this.dim = dim;
        this.data = data;
    }

    public KeyCollection getRowKeys() {
        return new KeySet();
    }

    public KeyCollection getColKeys() {
        return KeyRange.Single;
    }

    protected Matrix createEmptySubMatrix() {

        Long2DoubleHashMap hm = new Long2DoubleHashMap(dim);
        return hm;
    }

    @Override
    public Matrix subMatrix(KeyCollection otherKeys, KeyCollection colKeys) {
        Long2DoubleHashMap hm = (Long2DoubleHashMap) createEmptySubMatrix();

        Set<Long> keys = data.keySet();

        if (otherKeys.size() < keys.size()) {
            Iterator<Long> it = otherKeys.iterator();
            while (it.hasNext()) {
                long k = it.next();
                if (keys.contains(k))
                    hm.data.put(k, data.get(k));
            }
        } else {
            Iterator<Long> it = keys.iterator();
            while (it.hasNext()) {
                long k = it.next();
                if (otherKeys.contains(k))
                    hm.data.put(k, data.get(k));
            }
        }

        return hm;
    }

    @Override
    public boolean mergeMatrices(List<Matrix> matrices) {
        for (Matrix m : matrices) {
            mergeMatrix(m);
        }

        return true;
    }

    @Override
    public boolean mergeMatrix(Matrix m) {
        Long2DoubleHashMap hm = (Long2DoubleHashMap) m;

        for (long i : hm.data.keySet()) {
            data.put(i, hm.data.get(i));
        }
        return true;
    }
}
