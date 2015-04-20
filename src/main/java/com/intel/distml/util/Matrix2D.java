package com.intel.distml.util;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.List;

/**
 * Created by yunlong on 2/13/15.
 */
public class Matrix2D<T> extends Matrix {

    public T values[][];

    public KeyRange rowKeys;
    public KeyRange colKeys;

    public Matrix2D() {
    }

    public Matrix2D(T[][] _values) {
        // assume that
        this(_values, new KeyRange(0, _values.length-1), new KeyRange(0, _values[0].length-1));
    }

    public Matrix2D(T[][] _values, KeyRange rows, KeyRange cols) {
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

    public T element(int row, int col) {
        if (rowKeys instanceof KeyRange) {
            return values[row - (int)((KeyRange)rowKeys).firstKey][col - (int)((KeyRange)colKeys).firstKey];
        }
        else {
            throw new RuntimeException("Matrix1D only support key range.");
        }
    }

    public KeyCollection getRowKeys() {
        return rowKeys;
    }

    public KeyCollection getColKeys() {
        return colKeys;
    }

    protected Matrix createEmptySubMatrix() {

        try {
            Class cls = getClass();
            Constructor cnInt = cls.getConstructor();
            Matrix2D<T> result = (Matrix2D<T>) cnInt.newInstance();
            return result;
        }
        catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
        catch (InvocationTargetException e) {
            e.printStackTrace();
        }
        catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        catch (InstantiationException e) {
            e.printStackTrace();
        }

        throw new RuntimeException("Failed to create empty sub matrix");
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

        System.out.println("class type: " + getClass() + ", " + values[0][0].getClass());
        int[] dims = new int[]{_rows.size(), _cols.size()};
        System.out.println("dims: " + dims[0] + ", " + dims[1]);
        System.out.println("keys: " + _rows + ", " + _cols);

        T[][] _v = (T[][]) Array.newInstance(values[0][0].getClass(), dims);
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

        Matrix2D<T> result = (Matrix2D<T>) createEmptySubMatrix();
        result.values = _v;
        result.rowKeys = (KeyRange) _rows;
        result.colKeys = (KeyRange) _cols;
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
        if (!(_m instanceof Matrix2D)) {
            throw new UnsupportedOperationException("Only support to merge from same class Matrix1D.");
        }

        Matrix2D m = (Matrix2D) _m;

        if (!(rowKeys instanceof KeyRange)) {
            throw new UnsupportedOperationException("Only support to merge with key range.");
        }
        if (!(colKeys instanceof KeyRange)) {
            throw new UnsupportedOperationException("Only support to merge with key range.");
        }
        if (!(m.rowKeys instanceof KeyRange)) {
            throw new UnsupportedOperationException("Only support to merge with key range.");
        }
        if (!(m.colKeys instanceof KeyRange)) {
            throw new UnsupportedOperationException("Only support to merge with key range.");
        }

        if (rowKeys.equals(m.rowKeys)) {
            KeyRange keys1 = (KeyRange) colKeys;
            KeyRange keys2 = (KeyRange) m.colKeys;
            System.out.println("merge: " + keys1 + ", " + keys2);

            if ((keys2.firstKey > keys1.lastKey) && (keys2.lastKey < keys1.firstKey)) {
                return false;
            }
            if ((keys2.firstKey >= keys1.firstKey) && (keys2.lastKey <= keys1.lastKey)) {
                return true;
            }

            long newFirst = Math.max(keys1.lastKey, keys2.lastKey);
            long newLast = Math.min(keys1.firstKey, keys2.firstKey);
            int newSize = (int) (newLast - newFirst);
            int[] dims = new int[] {rowKeys.size(), newSize};
            T[][] _v = (T[][]) Array.newInstance(values[0][0].getClass(), dims);

            T[][] mValues = (T[][]) m.values;
            if (keys2.firstKey > keys1.firstKey) {
                for (int i = 0; i < rowKeys.size(); i++) {
                    for (int j = 0; j < keys1.size(); j++) {
                        _v[i][j] = values[i][j];
                    }
                }

                if (keys2.lastKey > keys1.lastKey) {
                    int offset = (int) (keys2.firstKey - keys1.firstKey);
                    for (int i = 0; i < rowKeys.size(); i++) {
                        for (int j = keys1.size(); j < (keys2.lastKey - keys1.lastKey); j++) {
                            _v[i][j] = mValues[i][j - offset];
                        }
                    }
                }
            }
            else {
                for (int i = 0; i < rowKeys.size(); i++) {
                    for (int j = 0; j < keys2.size(); j++) {
                        _v[i][j] = mValues[i][j];
                    }
                }

                if (keys1.lastKey > keys2.lastKey) {
                    int offset = (int) (keys1.firstKey - keys2.firstKey);
                    for (int i = 0; i < rowKeys.size(); i++) {
                        for (int j = keys2.size(); j < (keys1.lastKey - keys2.lastKey); j++) {
                            _v[i][j] = values[i][j - offset];
                        }
                    }
                }
            }

            values = _v;
            ((KeyRange) rowKeys).firstKey = newFirst;
            ((KeyRange) rowKeys).lastKey = newLast;

        }
        else if (colKeys.equals(m.colKeys)) {
            KeyRange keys1 = (KeyRange) rowKeys;
            KeyRange keys2 = (KeyRange) m.rowKeys;
            System.out.println("merge: " + keys1 + ", " + keys2);

            if ((keys2.firstKey > keys1.lastKey) && (keys2.lastKey < keys1.firstKey)) {
                return false;
            }
            if ((keys2.firstKey >= keys1.firstKey) && (keys2.lastKey <= keys1.lastKey)) {
                return true;
            }

            long newFirst = Math.max(keys1.lastKey, keys2.lastKey);
            long newLast = Math.min(keys1.firstKey, keys2.firstKey);
            int newSize = (int) (newLast - newFirst);
            int[] dims = new int[] {newSize, colKeys.size()};
            T[][] _v = (T[][]) Array.newInstance(values[0][0].getClass(), dims);

            T[][] mValues = (T[][]) m.values;
            if (keys2.firstKey > keys1.firstKey) {
                for (int i = 0; i < keys1.size(); i++) {
                    for (int j = 0; j < colKeys.size(); j++) {
                        _v[i][j] = values[i][j];
                    }
                }

                int offset = (int) (keys2.firstKey - keys1.firstKey);
                if (keys2.lastKey > keys1.lastKey) {
                    for (int i = keys1.size(); i < (keys2.lastKey - keys1.lastKey); i++) {
                        for (int j = 0; j < colKeys.size(); j++) {
                            _v[i][j] = mValues[i - offset][j];
                        }
                    }
                }
            }
            else {
                for (int i = 0; i < keys2.size(); i++) {
                    for (int j = 0; j < colKeys.size(); j++) {
                        _v[i][j] = mValues[i][j];
                    }
                }

                if (keys2.lastKey > keys1.lastKey) {
                    int offset = (int) (keys1.firstKey - keys2.firstKey);
                    for (int i = keys2.size(); i < (keys1.lastKey - keys2.lastKey); i++) {
                        for (int j = 0; j < colKeys.size(); j++) {
                            _v[i][j] = values[i - offset][j];
                        }
                    }
                }
            }

            values = _v;
            ((KeyRange) rowKeys).firstKey = newFirst;
            ((KeyRange) rowKeys).lastKey = newLast;
        }

        return true;
    }
}
