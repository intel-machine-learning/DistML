package com.intel.distml.model.cnn;

import com.intel.distml.util.KeyCollection;
import com.intel.distml.util.KeyRange;
import com.intel.distml.util.Matrix;

import java.util.List;

/**
 * Created by yunlong on 2/7/15.
 */
public class ImagesData extends Matrix {

    //
    float[][][] values;

    int imageWidth;
    int imageHeight;
    int imageNum;

    KeyCollection keys;

    public ImagesData(int imageWidth, int imageHeight, KeyCollection keys) {

        values = new float[keys.size()][imageWidth][imageHeight];

        this.imageHeight = imageHeight;
        this.imageWidth = imageWidth;
        this.keys = keys;
        this.imageNum = keys.size();
    }

    public float[][] element(long index) {
        int r = (int) (index - ((KeyRange) keys).firstKey);

        return values[r];
    }

    public KeyCollection getRowKeys() {
        return keys;
    }

    public KeyCollection getColKeys() {
        return KeyRange.Single;
    }

    public void init() {
    }

    @Override
    public void show() {
        System.out.println("values = " + values);
        System.out.println("rowKeys = " + keys);
        for (int i = 0; i < values.length; i++) {
            for (int j = 0; j < values[0].length; j++) {
                for(int k=0;k<values[0][0].length;k++)
                System.out.println("values[" + i + "][" + j + "]["+k+"] = " + values[i][j][k]);
            }
        }
    }
    ////////////////////////////////////////////////////////////////////////////////
    // Code below is for partitioning only. If the layer doesn't need partitioning,
    // the code can be removed.
    ////////////////////////////////////////////////////////////////////////////////

    public Matrix subMatrix(KeyCollection rowKeys, KeyCollection colKeys) {
        System.out.println("subMatrix: " + rowKeys + ", " + colKeys);
        if ((rowKeys.equals(KeyCollection.ALL)) && (colKeys.equals(KeyCollection.ALL))) {
            return this;
        }
        throw new RuntimeException("this method need child class to implement.");
    }

    @Override
    public boolean mergeMatrices(List<Matrix> matrices) {
        if (matrices.isEmpty()) {
            return true;
        }

        // todo implemented real merging
        System.out.println("Failed to merge all matrices, " + matrices.size() + " left.");
        return false;
    }

    private class SubMatrix extends Matrix {

        KeyCollection rows, cols;

        public SubMatrix(KeyCollection rows, KeyCollection cols) {
            this.rows = rows;
            this.cols = cols;
        }

        public KeyCollection getRowKeys() {
            return rows;
        }

        public KeyCollection getColKeys() {
            return cols;
        }
    }

    public void sigmoid(){
        for(int i=0;i<imageNum;i++)
            for(int j=0;j<imageHeight;j++)
                for(int k=0;k<imageWidth;k++)
                    values[i][j][k]=1.0f/(1.0f+(float)Math.exp(-values[i][j][k]));

    }



}
