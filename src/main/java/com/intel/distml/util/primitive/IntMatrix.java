package com.intel.distml.util.primitive;

import com.intel.distml.util.HashMapMatrix;
import com.intel.distml.util.KeyCollection;
import com.intel.distml.util.KeyRange;
import com.intel.distml.util.Matrix;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.List;

/**
 * Created by yunlong on 2/13/15.
 */
public class IntMatrix extends Matrix {

    public int values[][];

    public KeyRange rowKeys;
    public KeyRange colKeys;

    public IntMatrix() {
    }

    public IntMatrix(int[][] _values) {
        this(_values, new KeyRange(0, _values.length-1), new KeyRange(0, _values[0].length-1));
    }

    public IntMatrix(int[][] _values, KeyRange rows, KeyRange cols) {
        this.values = _values;
        rowKeys = rows;
        colKeys = cols;
    }

    public void show() {
        System.out.println("values = " + values);
        System.out.println("rowKeys = " + rowKeys);
        for (int i = 0; i < values.length; i++) {
            for (int j = 0; j < values[0].length; j++) {
                System.out.println("values[" + i + "][" + j + "] = " + values[i][j]);
            }
        }
    }

    public int element(int row, int col) {
        if (rowKeys instanceof KeyRange) {
            return values[row - (int)((KeyRange)rowKeys).firstKey][col - (int)((KeyRange)colKeys).firstKey];
        }
        else {
            throw new RuntimeException("Matrix1D only support key range.");
        }
    }

    public void setElement(int row, int col, int value) {
        values[row - (int)((KeyRange)rowKeys).firstKey][col - (int)((KeyRange)colKeys).firstKey] = value;
    }

    public KeyCollection getRowKeys() {
        return rowKeys;
    }

    public KeyCollection getColKeys() {
        return colKeys;
    }

    @Override
    protected Matrix createEmptySubMatrix() {
        return new IntMatrix();
    }

    public Matrix subMatrix(KeyCollection rows, KeyCollection cols) {

        System.out.println("submatrix with rows: " + rows);

        if ((rows.equals(KeyCollection.ALL)) && (cols.equals(KeyCollection.ALL))) {
            return this;
        }

        //todo How about rowKeys is ALL?
        KeyCollection _rows = rowKeys.intersect(rows);
        KeyCollection _cols = colKeys.intersect(cols);

        System.out.println("values[0][0] = " + values[0][0]);

        if (_rows instanceof KeyRange) {
            int[][] _v = new int[(int)_rows.size()][(int) _cols.size()];
            Iterator<Long> rIt = _rows.iterator();
            int i = 0;
            while (rIt.hasNext()) {
                int r = rIt.next().intValue();

                Iterator<Long> cIt = _cols.iterator();
                int j = 0;
                while (cIt.hasNext()) {
                    int c = cIt.next().intValue();
                    _v[i][j] = element(r, c);
                    j++;
                }
                i++;
            }

            IntMatrix result = (IntMatrix) createEmptySubMatrix();
            result.values = _v;
            result.rowKeys = (KeyRange) _rows;
            result.colKeys = (KeyRange) _cols;
            return result;
        }
        else { // suppose it is KeyList
            HashMapMatrix<int[]> hm = new HashMapMatrix<int[]>();
            Iterator<Long> it = _rows.iterator();
            while(it.hasNext()) {
                long key = it.next();
                int index = (int) (key - rowKeys.firstKey);
                hm.put(key, values[index]);
            }
            return hm;
        }
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
        if (!(_m instanceof IntMatrix)) {
            throw new UnsupportedOperationException("Only support to merge from same class Matrix1D.");
        }

        IntMatrix m = (IntMatrix) _m;

        if (!colKeys.equals(m.colKeys)) {
            throw new UnsupportedOperationException("Only support to merge with same cols.");
        }

        KeyRange keys1 = (KeyRange) rowKeys;
        KeyRange keys2 = (KeyRange) m.rowKeys;

        int[][] v1 = values;
        int[][] v2 = m.values;

        if (keys1.firstKey > keys2.firstKey) {
            KeyRange tk = keys1;
            keys1 = keys2;
            keys2 = tk;

            int[][] tv = v1;
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
        int[][] _v = new int[newSize][(int)colKeys.size()];

        for (int i = 0; i < keys1.size(); i++) {
            for (int j = 0; j < colKeys.size(); j++) {
                _v[i][j] = v1[i][j];
            }
        }

        int count = (int) (keys2.lastKey - keys1.lastKey);
        int offset1 = (int) keys1.size();
        int offset2 = (int) (keys1.lastKey - keys2.firstKey + 1);
        for (int i = 0; i < count; i++) {
            for (int j = 0; j < colKeys.size(); j++) {
                _v[i + offset1][j] = v2[i + offset2][j];
            }
        }

        values = _v;
        ((KeyRange) rowKeys).firstKey = keys1.firstKey;
        ((KeyRange) rowKeys).lastKey = keys2.lastKey;

        return true;
    }
}
