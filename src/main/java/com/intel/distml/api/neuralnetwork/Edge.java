package com.intel.distml.api.neuralnetwork;

import java.io.Serializable;

import com.intel.distml.api.databus.DataBus;

/**
 * Created by yunlong on 12/13/14.
 */
public abstract class Edge implements Serializable {

    // Cyclic reference is ok for java serialization:
    //  http://stackoverflow.com/questions/1792501/does-java-serialization-work-for-cyclic-references
    public Layer srcLayer;
    public Layer dstLayer;
    
    public abstract void computeForward(NeuralNetwork network, int workerIndex, DataBus dataBus);
    public abstract void computeBackward(NeuralNetwork network, int workerIndex, DataBus dataBus);

    public Edge(Layer src, Layer dst) {
        this.srcLayer = src;
        this.dstLayer = dst;
    }

}
