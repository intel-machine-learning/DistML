package com.intel.distml.model.cnn;

/**
 * Created by ruixiang on 15-3-10.
 */
public class CNNUtil {


    ////////////////////////////////////////////////////////////////////////////////
    // Helper Methods
    ////////////////////////////////////////////////////////////////////////////////
    public static void addBias2D(float[][] imagedata,float bias){
        for(int i=0;i<imagedata.length;i++)
            for(int j=0;j<imagedata[0].length;j++)
                imagedata[i][j]+=bias;
    }

    public static void expendEdge(float[][] input,float[][] output,int edge){
        for(int i=0;i<output.length;i++)
            for(int j=0;j<output[0].length;j++)
                output[i][j]=0.0f;

        for(int i=0;i<input.length;i++)
            for(int j=0;j<input[0].length;j++)
                output[i+edge][j+edge]=input[i][j];
    }
    public static void convolution2D(float [][] input, int width, int height,
                                     float [][] kernel, int kernelWidth, int kernelHeight, float[][] output) {

        int outputWidth = width - kernelWidth + 1;
        int outputHeight = height - kernelHeight + 1;

        for (int i = 0; i < outputWidth; ++i) {
            for (int j = 0;j < outputHeight; ++j) {
                output[i][j] = 0;
            }
        }

        for (int i = 0; i < outputWidth; ++i) {
            for (int j = 0; j < outputHeight; ++j) {
                //System.out.println("convolution2D: " + i + ", " + j);
                output[i][j] = singlePixelConvolution(input,i,j,kernel,
                        kernelWidth,kernelHeight);
            }
        }
    }
    public static void elementwiseAddition(float[][] summand,float[][] addend){
        if(summand.length!=addend.length||summand[0].length!=addend[0].length)return;
        for(int i=0;i<summand.length;i++)
            for(int j=0;j<addend.length;j++)
                summand[i][j]+=addend[i][j];
    }
    public static float singlePixelConvolution(float[][] input, int x, int y,
                                               float [][] k, int kernelWidth, int kernelHeight) {

        float output = 0;
        for (int i = 0; i < kernelWidth; ++i) {
            for (int j = 0; j < kernelHeight; ++j) {
                output = output + (input[x + i][y + j] * k[i][j]);
            }
        }
        return output;
    }

    public static void copyFrom(float[][][] input, float[][][] output){
        for(int i=0;i<input.length;i++)
            for(int j=0;j<input[0].length;j++)
                for(int k=0;k<input[0][0].length;k++)
                    output[i][j][k]=input[i][j][k];
    }
    public static void Multiply(float f,float[][][] output){
        for(int i=0;i<output.length;i++)
            for(int j=0;j<output[0].length;j++)
                for(int k=0;k<output[0][0].length;k++)
                    output[i][j][k]=f*output[i][j][k];
    }
    public static void elementwiseSubtration(float[][][] input, float[][][] output){
        for(int i=0;i<input.length;i++)
            for(int j=0;j<input[0].length;j++)
                for(int k=0;k<input[0][0].length;k++)
                    output[i][j][k]-=input[i][j][k];
    }
    public static void elementwiseMultipy(float[][][] input, float[][][] output){
        for(int i=0;i<input.length;i++)
            for(int j=0;j<input[0].length;j++)
                for(int k=0;k<input[0][0].length;k++)
                    output[i][j][k]*=input[i][j][k];
    }
    public static void Subtration(float r,float[][][] output){
        for(int i=0;i<output.length;i++)
            for(int j=0;j<output[0].length;j++)
                for(int k=0;k<output[0][0].length;k++)
                    output[i][j][k]=r-output[i][j][k];
    }
}
