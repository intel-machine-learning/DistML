package com.intel.distml.util.primitive;

import com.intel.distml.util.*;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.List;

/**
 * Created by yunlong on 2/13/15.
 */
public class IntArray extends Matrix {

    public int values[];

    public KeyCollection rowKeys;

    public IntArray() {
    }

    public IntArray(int dim) {
        values = new int[dim];
        rowKeys = new KeyRange(0, dim-1);
    }

    public IntArray(KeyCollection rows) {
        assert(rows instanceof KeyRange);

        values = new int[(int)rows.size()];
        this.rowKeys = rows;
    }

    public IntArray(int[] _values) {
        this(_values, new KeyRange(0, _values.length-1));
    }

    public IntArray(int[] _values, KeyRange rows) {
        this.values = _values;
        rowKeys = rows;
    }

    public void show() {
        System.out.println("values = " + values);
        System.out.println("rowKeys = " + rowKeys);
        for (int i = 0; i < values.length; i++) {
            System.out.println("values[" + i + "] = " + values[i]);
        }
    }

    public int element(int key) {
        if (rowKeys instanceof KeyRange) {
            int index = key - (int)((KeyRange)rowKeys).firstKey;
            return values[index];
        }
        else if (rowKeys instanceof KeyHash) {
            rowKeys.contains(key);
            int index = key / ((KeyHash)rowKeys).hashQuato;
            return values[index];
        }
        else {
            throw new RuntimeException("not supported keys.");
        }
    }

    public void setElement(long key, int value) {
        if (rowKeys instanceof KeyRange) {
            int index = (int) (key - ((KeyRange)rowKeys).firstKey);
            values[index] = value;
        }
        else if (rowKeys instanceof KeyHash) {
            rowKeys.contains(key);
            int index = (int) (key / ((KeyHash)rowKeys).hashQuato);
            values[index] = value;
        }
        else {
            throw new RuntimeException("not supported keys.");
        }
    }

    public KeyCollection getRowKeys() {
        return rowKeys;
    }

    public KeyCollection getColKeys() {
        return KeyRange.Single;
    }

    protected Matrix createEmptySubMatrix() {
        return new IntArray();
    }

    public Matrix subMatrix(KeyCollection rows, KeyCollection cols) {

        System.out.println("submatrix with rows: " + rows);

        if (rows.equals(KeyCollection.ALL)) {
            return this;
        }

        if (!(rows instanceof KeyRange)) {
            HashMapMatrix<Integer> output = new HashMapMatrix<Integer>();
            Iterator<Long> it = rows.iterator();
            while(it.hasNext()) {
                long key = it.next();
                if (rowKeys.contains(key)) {
                    output.put(key, element((int)key));
                }
            }
            return output;
        }

        KeyCollection _rows = rowKeys.intersect(rows);
        IntArray result = new IntArray(_rows);

        Iterator<Long> it = _rows.iterator();
        while(it.hasNext()) {
            int key = it.next().intValue();
            result.setElement(key, element(key));
        }

        return result;
    }

    @Override
    public boolean mergeMatrices(List<Matrix> matrices) {

        boolean merged = true;
        while(merged) {
            merged = false;
            int i = 0;
            while (i < matrices.size()) {
                Matrix m = matrices.get(i);
                if (mergeMatrix(m)) {
                    matrices.remove(i);
                    merged = true;
                }
                i++;
            }
        }

        if (matrices.size() > 0) {
            System.out.println("Failed to merge all matrices, " + matrices.size() + " left.");
            return false;
        }

        return true;
    }

    @Override
    public boolean mergeMatrix(Matrix _m) {
        System.out.println("mergeMatrix in General Array");
        if (_m.getRowKeys().isEmpty()) {
            return true;
        }

        if (_m instanceof HashMapMatrix) {
            return mergeHashMapMatrix((HashMapMatrix)_m);
        }

        if (!(_m instanceof IntArray)) {
            throw new UnsupportedOperationException("Only support to merge from same class Matrix1D: " + _m);
        }

        IntArray m = (IntArray) _m;
        KeyRange keys1 = (KeyRange) rowKeys;
        KeyRange keys2 = (KeyRange) m.rowKeys;

        int[] v1 = values;
        int[] v2 = m.values;

        if (keys1.firstKey > keys2.firstKey) {
            KeyRange tk = keys1;
            keys1 = keys2;
            keys2 = tk;

            int[] tv = v1;
            v1 = v2;
            v2 = tv;
        }

        System.out.println("merge: " + keys1 + ", " + keys2);


        if (keys2.firstKey > keys1.lastKey + 1) {
            return false;
        }
        if (keys1.lastKey >= keys2.lastKey) {
            return true;
        }

        int newSize = (int) (keys2.lastKey - keys1.firstKey + 1);
        int[] _v = new int[newSize];

        System.arraycopy(_v, 0, v1, 0, (int)keys1.size());
        System.arraycopy(_v, (int)keys1.size(), v2, (int)(keys1.lastKey - keys2.firstKey + 1), (int)(keys2.lastKey - keys1.lastKey));

        values = _v;
        ((KeyRange) rowKeys).firstKey = keys1.firstKey;
        ((KeyRange) rowKeys).lastKey = keys2.lastKey;

        return true;
    }

    public boolean mergeHashMapMatrix(HashMapMatrix<Integer> _m) {

        Iterator<Long> it = _m.getRowKeys().iterator();
        while(it.hasNext()) {
            long key = it.next();
            int value = _m.get(key);
            setElement(key, value);
        }

        return true;
    }
}
