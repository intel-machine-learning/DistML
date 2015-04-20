package com.intel.distml.model.cnn;

import com.intel.distml.util.*;

/**
 * Created by yunlong on 2/7/15.
 */
public class FullConnectionMatrix extends Matrix2D<Float> {

    public FullConnectionMatrix() {
    }

    public FullConnectionMatrix(int inputCount, KeyRange rowKeys) {
        super(new Float[rowKeys.size()][inputCount+1], rowKeys, new KeyRange(0, inputCount));//TODO:last is bias
    }

    public void initWithValue(float f) {
        System.out.println("init: " + rowKeys + ", " + colKeys);
        for (int i = 0; i < rowKeys.size(); i++) {
            for (int j = 0; j < colKeys.size()-1; j++) {
                values[i][j] = f;
            }
            this.values[i][colKeys.size()-1]=0.0f;
        }
    }
    public void initRandom() {
        System.out.println("init: " + rowKeys + ", " + colKeys);
        for (int i = 0; i < rowKeys.size(); i++) {
            for (int j = 0; j < colKeys.size()-1; j++) {
                values[i][j] = (float)((Math.random()-0.5)*2*Math.sqrt(6.0/(10+192)));
            }
            this.values[i][colKeys.size()-1]=0.0f;
        }
    }

    public void calculate(ImagesData images, Matrix1D<Float> output) {

        int imageSize = images.imageWidth * images.imageHeight;
        int cStart = (int) ((KeyRange) colKeys).firstKey/imageSize;
        int cEnd = (int) ((KeyRange) colKeys).lastKey/imageSize;

        // matlab code
        //net.o = sigm(net.ffW * net.fv + repmat(net.ffb, 1, size(net.fv, 2)));

        Float[] vector = output.values;
        for (int i = 0; i < rowKeys.size(); i++) {
            Float[] weights = values[i];

            float sum = 0.0f;
            for (int j = cStart; j < cEnd; j++) {
                float[][] image = images.element(j);
                for (int m = 0; m < images.imageWidth; m++) {
                    for (int n = 0; n < images.imageHeight; n++) {
                        sum += image[m][n] * weights[j + m * images.imageWidth + n];
                    }
                }
            }
            sum+=this.values[i][colKeys.size()-1];
            vector[i] = sigmoid(sum);
        }
    }

    private float sigmoid(float val) {
        return (float) (1.0f / (1.0f + Math.exp(-val)));
    }


}
