package com.intel.distml.util;

import scala.tools.nsc.Global;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Created by yunlong on 2/1/15.
 */
public class HashMapMatrix<T> extends Matrix implements Cloneable {

    public HashMap<Long, T> data;
//    public long dim;

    public class KeySet extends KeyCollection {

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

        public long size() {
            return data.size();
        }
/*
        public PartitionInfo partitionEqually(int hostNum) {
            throw new UnsupportedOperationException("This class is internally used only.");
        }
*/
        public KeyCollection[] split(int hostNum) {
            KeyList[] sets = new KeyList[hostNum];

            long step = (data.keySet().size() - 1 + hostNum) / hostNum;
            Iterator<Long> it = data.keySet().iterator();
            int setIndex = 0;
            sets[setIndex] = new KeyList();
            int counter = 0;
            while(it.hasNext()) {
                sets[setIndex].addKey(it.next());
                counter++;
                if (counter == step) {
                    counter = 0;
                    setIndex++;
                    if (it.hasNext()) {
                        sets[setIndex] = new KeyList();
                    }
                }
            }

            return sets;
        }
    }

    public HashMapMatrix(/*long dim*/) {
        data = new HashMap<Long, T>();
        //this.dim = dim;
    }

    public HashMapMatrix(int initialCapacity) {
        data = new HashMap<Long, T>(initialCapacity);
        //this.dim = dim;
    }

    public HashMapMatrix(HashMap<Long, T> data) {
        this.data = data;
    }

    public T get(long key) {
        return data.get(key);
    }

    public void put(long key, T value) {
        data.put(key, value);
    }

    public KeyCollection getRowKeys() {
        return new KeySet();
    }

    public KeyCollection getColKeys() {
        return KeyRange.Single;
    }

    public int size() {
        return data.size();
    }

    protected Matrix createEmptySubMatrix() {
        HashMapMatrix hm = new HashMapMatrix();

        return hm;
    }


    @Override
    public Matrix subMatrix(KeyCollection otherKeys, KeyCollection colKeys) {

        if (otherKeys instanceof KeyCollection.ALL_KEYS) {
            return this;
        }

        HashMapMatrix hm = (HashMapMatrix) createEmptySubMatrix();

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

        System.out.println("submatrix: " + hm.size());
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
        HashMapMatrix<T> hm = (HashMapMatrix<T>) m;
        for (long i : hm.data.keySet()) {
            data.put(i, hm.data.get(i));
        }

        return true;
    }

    public void plus(HashMapMatrix<T> m, Plus<T> func) {
        for (long k : m.data.keySet()) {
            if (data.containsKey(k)) {
                T t1 = data.get(k);
                T t2 = m.data.get(k);
                data.put(k, func.plus(t1, t2));
            }
            else {
                data.put(k, m.data.get(k));
            }
        }
    }

}
