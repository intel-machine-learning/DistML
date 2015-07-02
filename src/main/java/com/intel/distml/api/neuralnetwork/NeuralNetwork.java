package com.intel.distml.api.neuralnetwork;

import com.intel.distml.api.Model;
import com.intel.distml.api.databus.DataBus;
import com.intel.distml.util.Matrix;

/**
 * Created by yunlong on 3/1/15.
 */
public class NeuralNetwork extends Model {

    public Layer[] layers;
    public boolean bpNeeded;
    public Matrix sample;

    public NeuralNetwork(int layerCount) {
        this(layerCount, true);
    }

    public NeuralNetwork(/* int psNum, int workerGroupSize, */ int layerCount, boolean bpNeeded) {
        super();

        this.bpNeeded = bpNeeded;

        layers = new Layer[layerCount];
    }

    public Layer getLayer(int index) {
        return layers[index];
    }

    public Layer getLayer(String name) {
        for (Layer l : layers) {
            if (l.name.equals(name)) {
                return l;
            }
        }

        return null;
    }

    public void addLayer(int index, Layer layer) {
        layer.index = index;
        layers[index] = layer;
    }

    @Override
    public void compute(Matrix sample, int workerIndex, DataBus dataBus) {

        this.sample = sample;
        ((InputLayer)getLayer(0)).setSample(sample);

            for (int i = 1; i < layers.length; i++) {
                Layer layer = layers[i];
                for (int j = 0; j < layer.edges.size(); j++) {
                    Edge e = layer.edges.get(j);
                    e.computeForward(this, workerIndex, dataBus);
                }
            }

            if (bpNeeded) {
                System.out.println("bp now");
                for (int i = layers.length - 1; i > 0; i--) {
                    Layer layer = layers[i];
                    for (int j = 0; j < layer.edges.size(); j++) {
                        Edge e = layer.edges.get(j);
                        e.computeBackward(this, workerIndex, dataBus);
                    }
                }
            }

            System.out.println("sample done.");
    }

    @Override
    public void mergeUpdate(int serverIndex, String matrixName, Matrix update) {

        String layerName = Layer.getLayerName(matrixName);

        Layer layer = getLayer(layerName);

        layer.mergeUpdate(update);
    }

}
