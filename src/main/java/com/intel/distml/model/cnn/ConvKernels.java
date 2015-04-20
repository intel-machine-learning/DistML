package com.intel.distml.model.cnn;

import com.intel.distml.util.*;

import java.util.Iterator;
import java.util.List;

/**
 * Created by yunlong on 2/7/15.
 */
public class ConvKernels extends Matrix {

    //
    float[][][][] values;
    float[] bias;

    int inputCount;
    int outputCount;
    int kernalWidth;
    int kernalHeight;
    KeyCollection rowKeys, colKeys;

    public ConvKernels(int kernalWidth, int kernalHeight, int inputCount, KeyRange rowKeys) {
        this.values = new float[rowKeys.size()][inputCount][kernalWidth][kernalHeight];
        this.bias=new float[rowKeys.size()];
        this.rowKeys = rowKeys;
        this.colKeys = new KeyRange(0, inputCount - 1);
        this.kernalHeight = kernalHeight;
        this.kernalWidth = kernalWidth;

        this.inputCount = inputCount;
        this.outputCount = rowKeys.size();
    }

    public KeyCollection getRowKeys() {
        return rowKeys;
    }

    public KeyCollection getColKeys() {
        return colKeys;
    }

    public float[][] element(long row, long col) {
        int r = (int) (row - ((KeyRange) rowKeys).firstKey);
        int c = (int) (col - ((KeyRange) colKeys).firstKey);

        return values[r][c];
    }

    public void initRandom(int totalInput, int totalOutput) {
        for (int i = 0; i < outputCount; i++) {
            for (int j = 0;j < inputCount; j++) {
                for (int k = 0; k < kernalWidth; k++) {
                    for (int l = 0; l < kernalHeight; l++) {
                        values[i][j][k][l] = (float) ((Math.random() - 0.5) * 2 * Math.sqrt(6.0 / (totalInput + totalOutput)));
                    }
                }
            }
        }
        for(int i=0;i<outputCount;i++) bias[i]=0.0f;
    }

    public void initWithValue(float f){
        for (int i = 0; i < outputCount; i++) {
            for (int j = 0;j < inputCount; j++) {
                for (int k = 0; k < kernalWidth; k++) {
                    for (int l = 0; l < kernalHeight; l++) {
                        values[i][j][k][l] = f;
                    }
                }
            }
        }
        for(int i=0;i<outputCount;i++) bias[i]=0.0f;
    }

    public void conv(ImagesData images, ImagesData output) {
        System.out.println("conv images: " + rowKeys + ", " + colKeys);

        long rStart = ((KeyRange) rowKeys).firstKey;
        long rEnd = ((KeyRange) rowKeys).lastKey;

        long cStart = ((KeyRange) colKeys).firstKey;
        long cEnd = ((KeyRange) colKeys).lastKey;


        for (long i = rStart; i <= rEnd; i++) {
            float[][] outImage = output.element(i);
            for (long j = cStart; j <= cEnd; j++) {
                //System.out.println("conv: " + i + ", " + j);
                float[][] kernel = element(i, j);
                float[][] image = images.element(j);
                float[][] addend=new float[outImage.length][outImage[0].length];//only for temp;
                CNNUtil.convolution2D(image, images.imageWidth, images.imageHeight, kernel, kernalWidth, kernalHeight, addend);
                CNNUtil.elementwiseAddition(outImage, addend);
            }
            CNNUtil.addBias2D(outImage,bias[(int)i]);
        }
    }

    public void mergeUpdate(ConvKernels update) {
        Iterator<Long> rIt = update.getRowKeys().iterator();
        while (rIt.hasNext()) {
            Iterator<Long> cIt = update.getColKeys().iterator();
            while (cIt.hasNext()) {
                long r = rIt.next();
                long c = cIt.next();
                float[][] u = update.element(r, c);
                float[][] k = element(r, c);

                for (int i = 0; i < kernalWidth; i++)
                    for (int j = 0; j < kernalHeight; j++)
                        k[i][j] += u[i][j];
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Code below is for partitioning only. If the layer doesn't need partitioning,
    // the code can be removed.
    ////////////////////////////////////////////////////////////////////////////////

    public Matrix subMatrix(KeyCollection rowKeys, KeyCollection colsKeys) {
        // currently conv kernels don't need partitioning, so we simply return whole parameters
        return this;
//        throw new RuntimeException("this method need child class to implement :  " + getClass());
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

}
