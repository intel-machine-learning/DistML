package com.intel.distml.util;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.List;

/**
 * Created by yunlong on 2/13/15.
 */
public class Matrix1D<T> extends Matrix {

    public T values[];

    public int dim;
    public KeyCollection rowKeys;

    public Matrix1D() {
    }

    public Matrix1D(T t, int dim) {
        this.dim = dim;
        values = (T[]) Array.newInstance(t.getClass(), dim);
        rowKeys = new KeyRange(0, dim-1);
    }

    public Matrix1D(T[] _values) {
        this(_values, new KeyRange(0, _values.length-1));
    }

    public Matrix1D(T[] _values, KeyRange rows) {
        this.values = _values;
        rowKeys = rows;
        this.dim = rows.size();
    }

    public void show() {
        System.out.println("values = " + values);
        System.out.println("rowKeys = " + rowKeys);
        for (int i = 0; i < values.length; i++) {
            System.out.println("values[" + i + "] = " + values[i]);
        }
    }

    public T element(int key) {
        if (rowKeys instanceof KeyRange) {
            return values[key - (int)((KeyRange)rowKeys).firstKey];
        }
        else {
            throw new RuntimeException("Matrix1D only support key range.");
        }
    }

    public KeyCollection getRowKeys() {
        return rowKeys;
    }

    public KeyCollection getColKeys() {
        return KeyRange.Single;
    }

    protected Matrix createEmptySubMatrix() {

        try {
            Class cls = getClass();
            Constructor cnInt = cls.getConstructor();
            Matrix1D<T> result = (Matrix1D<T>) cnInt.newInstance();
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

        if (rows.equals(KeyCollection.ALL)) {
            return this;
        }

        if (rows instanceof KeyList) {
            HashMapMatrix<T> output = new HashMapMatrix<T>(dim);
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

            System.out.println("class type: " + getClass() + ", " + values[0].getClass());

            T[] _v = (T[]) Array.newInstance(values[0].getClass(), _rows.size());
            Iterator<Long> it = _rows.iterator();
            int index = 0;
            while(it.hasNext()) {
                int key = it.next().intValue();
                _v[index] = element(key);
                index++;
            }
            System.out.println("obj: " + _v.getClass() + ", " + _v[0]);


            Matrix1D<T> result = (Matrix1D<T>) createEmptySubMatrix();
            result.values = _v;
            result.rowKeys = _rows;
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
        if (!(_m instanceof Matrix1D)) {
            throw new UnsupportedOperationException("Only support to merge from same class Matrix1D.");
        }

        Matrix1D m = (Matrix1D) _m;

        if (!(rowKeys instanceof KeyRange)) {
            throw new UnsupportedOperationException("Only support to merge with key range.");
        }
        if (!(m.rowKeys instanceof KeyRange)) {
            throw new UnsupportedOperationException("Only support to merge with key range.");
        }

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
        T[] _v = (T[]) Array.newInstance(values[0].getClass(), newSize);

        if (keys2.firstKey > keys1.firstKey) {
            KeyRange tmp = keys1;
            keys1 = keys2;
            keys2 = tmp;
        }

        System.out.println("copy: " + 0 + ", " + m.values.length);
        System.arraycopy(_v, 0, m.values, 0, m.values.length);
        if (keys2.lastKey < keys1.lastKey) {
            System.out.println("copy: " + m.values.length + ", " + (keys2.lastKey - keys1.firstKey + 1) + ", " + (keys1.lastKey - keys2.lastKey));
            System.arraycopy(_v, m.values.length, values, (int) (keys2.lastKey - keys1.firstKey + 1),
                    (int) (keys1.lastKey - keys2.lastKey));
        }

        values = _v;
        ((KeyRange) rowKeys).firstKey = newFirst;
        ((KeyRange) rowKeys).lastKey = newLast;

        return true;
    }

    public void elementwiseAddition(Matrix1D m){
        if(this.values.length!=m.values.length)return;
        Float[] s=(Float[])this.values;
        Float[] a=(Float[])m.values;
        for(int i=0;i<this.values.length;i++)
            s[i]=s[i]+a[i];
    }
    public void elementwiseSubtration(Matrix1D m){
        if(this.values.length!=m.values.length)return;
        Float[] s=(Float[])this.values;
        Float[] a=(Float[])m.values;
        for(int i=0;i<this.values.length;i++)
            s[i]=s[i]-a[i];
    }
    public void elementwiseMultipy(Matrix1D m){
        if(this.values.length!=m.values.length)return;
        Float[] s=(Float[])this.values;
        Float[] a=(Float[])m.values;
        for(int i=0;i<this.values.length;i++)
            s[i]=s[i]*a[i];
    }
    public void Subtration(Float r){
        Float[] t=(Float[])this.values;
        for(int i=0;i<this.values.length;i++)
            t[i]=r-t[i];
    }
    public void CopyFrom(Matrix1D m){
        if(this.values.length!=m.values.length)return;
        Float[] s=(Float[])this.values;
        Float[] a=(Float[])m.values;
        for(int i=0;i<this.values.length;i++)
            s[i]=a[i];
    }

}
