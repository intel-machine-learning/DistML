package com.intel.distml.api.neuralnetwork;

import java.io.Serializable;
import java.util.LinkedList;

import com.intel.distml.api.DMatrix;
import com.intel.distml.util.Matrix;

/**
 * Created by yunlong on 12/13/14.
 */
public abstract class Layer implements Serializable {

    public String name;
    public int index;

    public LinkedList<Edge> edges;

    public NeuralNetwork model;

    public Layer(NeuralNetwork model, String name) {

        this.model = model;
        this.name = name;

        edges = new LinkedList<Edge>();
    }

    public void addEdge(Edge e) {
        edges.add(e);
    }

    public DMatrix getMatrix(String matrixName) {
        return model.getMatrix(getGlobalName(matrixName));
    }

    public Matrix getCache(String matrixName) {
        return model.getMatrix(getGlobalName(matrixName)).localCache;
    }

    public void registerMatrix(String matrixName, DMatrix dmatrix) {
        model.registerMatrix(name + "." + matrixName, dmatrix);
    }

    public String getGlobalName(String matrixName) {
        return name + "." + matrixName;
    }

    public static String getLayerName(String globalName) {
        return globalName.split(".")[0];
    }

    public void mergeUpdate(Matrix update) {

    }
}
