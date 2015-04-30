package com.intel.distml.util;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.List;

/**
 * Created by yunlong on 2/13/15.
 */
public class GeneralArray<T> extends Matrix {

    public T values[];

//    public int dim;
    public KeyCollection rowKeys;

    public GeneralArray() {
    }

    public GeneralArray(T t, int dim) {
        //this.dim = dim;
        values = (T[]) Array.newInstance(t.getClass(), dim);
        rowKeys = new KeyRange(0, dim-1);
    }

    public GeneralArray(T t, KeyCollection rowKeys) {
        //this.dim = dim;
        values = (T[]) Array.newInstance(t.getClass(), rowKeys.size());
        this.rowKeys = rowKeys;
    }

    public GeneralArray(T[] _values) {
        this(_values, new KeyRange(0, _values.length-1));
    }

    public GeneralArray(T[] _values, KeyRange rows) {
        this.values = _values;
        rowKeys = rows;
//        this.dim = rows.size();
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
            GeneralArray<T> result = (GeneralArray<T>) cnInt.newInstance();
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
            HashMapMatrix<T> output = new HashMapMatrix<T>();
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


            GeneralArray<T> result = (GeneralArray<T>) createEmptySubMatrix();
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
        if (!(_m instanceof GeneralArray)) {
            throw new UnsupportedOperationException("Only support to merge from same class Matrix1D.");
        }

        GeneralArray m = (GeneralArray) _m;

        if (!(rowKeys instanceof KeyRange)) {
            throw new UnsupportedOperationException("Only support to merge with key range.");
        }
        if (!(m.rowKeys instanceof KeyRange)) {
            throw new UnsupportedOperationException("Only support to merge with key range.");
        }

        KeyRange keys1 = (KeyRange) rowKeys;
        T[] v1 = values;
        KeyRange keys2 = (KeyRange) m.rowKeys;
        T[] v2 = (T[]) m.values;
        //System.out.println("merge: " + keys1 + ", " + keys2 + ", " + values[0]);

        // fail for non-joint keys
        if ((keys2.firstKey > keys1.lastKey+1) || (keys1.firstKey > keys2.lastKey+1)) {
            System.out.println("merge fail: not joint ");
            return false;
        }

        // if one range is a child of the other, do nothing
        if ((keys2.firstKey >= keys1.firstKey) && (keys2.lastKey <= keys1.lastKey)) {
            System.out.println("no merge needed ");
            return true;
        }

        //System.out.println("mergine... ");
        // make sure keys1 is in front of keys2
        if (keys1.firstKey > keys2.firstKey) {
            KeyRange tmp = keys1;
            keys1 = keys2;
            keys2 = tmp;

            T[] tmpData = v1;
            v1 = v2;
            v2 = tmpData;
        }

        long newLast = Math.max(keys1.lastKey, keys2.lastKey);
        long newFirst = Math.min(keys1.firstKey, keys2.firstKey);
        int newSize = (int) (newLast - newFirst + 1);
        T[] _v = (T[]) Array.newInstance(values[0].getClass(), newSize);

        System.arraycopy(v1, 0, _v, 0, v1.length);
        System.arraycopy(v2, (int) (keys1.lastKey - keys2.firstKey + 1), _v, v1.length, (int) (keys2.lastKey - keys1.lastKey));

        values = _v;
        ((KeyRange) rowKeys).firstKey = newFirst;
        ((KeyRange) rowKeys).lastKey = newLast;
        //System.out.println("merge done: " + newFirst + ", " + newLast + ", " + values[0] + ", " + values[(int)newLast-1]);

        return true;
    }

    public void elementwiseAddition(GeneralArray m){
        if(this.values.length!=m.values.length)return;
        Float[] s=(Float[])this.values;
        Float[] a=(Float[])m.values;
        for(int i=0;i<this.values.length;i++)
            s[i]=s[i]+a[i];
    }
    public void elementwiseSubtration(GeneralArray m){
        if(this.values.length!=m.values.length)return;
        Float[] s=(Float[])this.values;
        Float[] a=(Float[])m.values;
        for(int i=0;i<this.values.length;i++)
            s[i]=s[i]-a[i];
    }
    public void elementwiseMultipy(GeneralArray m){
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
    public void CopyFrom(GeneralArray m){
        if(this.values.length!=m.values.length)return;
        Float[] s=(Float[])this.values;
        Float[] a=(Float[])m.values;
        for(int i=0;i<this.values.length;i++)
            s[i]=a[i];
    }

}
