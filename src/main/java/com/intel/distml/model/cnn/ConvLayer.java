package com.intel.distml.model.cnn;

import com.intel.distml.api.*;
import com.intel.distml.api.neuralnetwork.NeuralNetwork;
import com.intel.distml.util.*;

public class ConvLayer extends ImageLayer {

	private class ConvNodes extends DMatrix {

		public ConvNodes(int flags) {
			super(flags, imageNum);
		}

		@Override
		public void initOnServer(int psIndex, KeyCollection keys) {
			ConvKernels data = new ConvKernels(kernelWidth, kernelHeight, input.imageNum, (KeyRange) keys);
			//data.initRandom(input.imageNum, imageNum);
			data.initWithValue(0.002105f);

			setLocalCache(data);
		}

		@Override
		public void initOnWorker(int workerIndex, KeyCollection keys) {
			ConvKernels data = new ConvKernels(kernelWidth, kernelHeight, input.imageNum, (KeyRange) keys);
			setLocalCache(data);
		}

		public void mergeUpdate(int serverIndex, Matrix update) {
			((ConvKernels) localCache).mergeUpdate((ConvKernels) update);
		}
	}

	ImageLayer input;
	int kernelWidth;
	int kernelHeight;

	public ConvLayer(NeuralNetwork model, String name, ImageLayer input, int kernelWidth, int kernelHeight, int outputNum) {
		super(model, name, input.imageWidth - kernelWidth + 1, input.imageHeight - kernelHeight + 1, outputNum);

		this.kernelWidth = kernelWidth;
		this.kernelHeight = kernelHeight;
		this.imageNum = outputNum;
		this.input = input;

		imageWidth = input.imageWidth - kernelWidth + 1;
		imageHeight = input.imageHeight - kernelHeight + 1;

		ConvEdge edge = new ConvEdge(input, this);
		addEdge(edge);

		registerMatrix(Model.MATRIX_PARAM, new ConvNodes(DMatrix.FLAG_PARAM | DMatrix.FLAG_ON_SERVER));
		registerMatrix(Model.MATRIX_UPDATE, new ConvNodes(DMatrix.FLAG_ON_WORKER));
		registerMatrix(Model.MATRIX_DATA, new ImageNodes());
		registerMatrix(Model.MATRIX_DELTA, new ImageNodes());

	}

	@Override
	public void mergeUpdate(Matrix update) {
		ConvKernels kernels = (ConvKernels) ((ConvNodes) getMatrix(Model.MATRIX_PARAM)).localCache;
		kernels.mergeUpdate((ConvKernels) update);
	}

	public static float singlePixelConvolution(float[][] input,
											   int x, int y,
											   float[][] k,
											   int kernelWidth,
											   int kernelHeight) {
		float output = 0;
		for (int i = 0; i < kernelWidth; ++i) {
			for (int j = 0; j < kernelHeight; ++j) {
				output = output + (input[x + i][y + j] * k[i][j]);
			}
		}
		return output;
	}


	public static float[][] convolution2D(float[][] input,
										  int width, int height,
										  float[][] kernel,
										  int kernelWidth,
										  int kernelHeight) {
		int smallWidth = width - kernelWidth + 1;
		int smallHeight = height - kernelHeight + 1;
		float[][] output = new float[smallWidth][smallHeight];
		for (int i = 0; i < smallWidth; ++i) {
			for (int j = 0; j < smallHeight; ++j) {
				output[i][j] = 0;
			}
		}
		for (int i = 0; i < smallWidth; ++i) {
			for (int j = 0; j < smallHeight; ++j) {
				output[i][j] = singlePixelConvolution(input, i, j, kernel,
						kernelWidth, kernelHeight);
				//if (i==32- kernelWidth + 1 && j==100- kernelHeight + 1) System.out.println("Convolve2D: "+output[i][j]);
			}
		}
		return output;
	}

	public static float singlePixelConvolution(float[][][] input, int x, int y, int z,
											   float[][][] kernal, int kernalWidth, int kernalHeight, int pageNum) {
		float output = 0;
		for (int i = 0; i < kernalWidth; i++)
			for (int j = 0; j < kernalHeight; j++)
				for (int k = 0; k < pageNum; k++) {
					output = output + input[x + i][y + j][z + k] * kernal[i][j][k];
				}

		return output;
	}

	public static float[][][] convolution3D(float[][][] input, float[][][] kernal) {
		int smallWidth = input.length - kernal.length + 1;
		int smallHeight = input[0].length - kernal[0].length + 1;
		int smallPage = input[0][0].length - kernal[0][0].length + 1;
		float[][][] output = new float[input.length - kernal.length + 1][input[0].length - kernal[0].length + 1][input[0][0].length - kernal[0][0].length + 1];
		for (int i = 0; i < smallWidth; i++)
			for (int j = 0; j < smallHeight; j++)
				for (int k = 0; k < smallPage; k++) {
					output[i][j][k] = singlePixelConvolution(input, i, j, k, kernal, kernal.length, kernal[0].length, kernal[0][0].length);
				}
		return output;
	}

	public static float[][][] convolution32D(float[][][] input, float[][] kernal) {
		float[][][] output = new float[input[0][0].length - kernal[0].length + 1][input[0].length - kernal.length + 1][input.length];
		for (int i = 0; i < input.length; i++) {
			output[i] = convolution2D(input[i], input.length, input[0].length, kernal, kernal.length, kernal[0].length);
		}
		return output;
	}

	public static float[] convolutionDouble(float[][] input,
											int width, int height,
											float[][] kernel,
											int kernelWidth, int kernelHeight) {
		int smallWidth = width - kernelWidth + 1;
		int smallHeight = height - kernelHeight + 1;
		float[][] small = new float[smallWidth][smallHeight];
		small = convolution2D(input, width, height, kernel, kernelWidth, kernelHeight);
		float[] result = new float[smallWidth * smallHeight];
		for (int j = 0; j < smallHeight; ++j) {
			for (int i = 0; i < smallWidth; ++i) {
				result[j * smallWidth + i] = small[i][j];
			}
		}
		return result;
	}
}
