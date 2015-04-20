package com.intel.distml.util;

import java.util.List;

/**
 * Created by yunlong on 2/13/15.
 */
public class Matrix0D<T> extends Matrix {

    public T value;

    public Matrix0D() {
    }

    public Matrix0D(T t) {
        this.value = t;
    }

    public T element() {
        return value;
    }

    public KeyCollection getRowKeys() {
        return KeyRange.Single;
    }

    public KeyCollection getColKeys() {
        return KeyRange.Single;
    }

    protected Matrix createEmptySubMatrix() {
        Matrix0D hm = new Matrix0D();

        return hm;
    }

    public Matrix subMatrix(KeyCollection rows, KeyCollection cols) {
        return this;
    }

    @Override
    public boolean mergeMatrices(List<Matrix> matrices) {
        return true;
    }

    @Override
    public boolean mergeMatrix(Matrix _m) {
        return true;
    }
}
