package com.intel.distml.api.neuralnetwork;

import com.intel.distml.api.DMatrix;
import com.intel.distml.api.Model;
import com.intel.distml.api.ModelWriter;
import com.intel.distml.api.databus.ServerDataBus;
import com.intel.distml.util.KeyCollection;
import com.intel.distml.util.Logger;
import com.intel.distml.util.Matrix;

/**
 * Created by yunlong on 2/1/15.
 */
public class NNModelWriter implements ModelWriter {

    public void writeModel(Model model, ServerDataBus dataBus) {
        NeuralNetwork nn = (NeuralNetwork) model;

        for (int i = 0; i < nn.layers.length; i++) {
            Layer l = nn.getLayer(i);
            Logger.DebugLog("model layer: " + i + ", " + l.name + ", " + l, Logger.Role.APP, 0);
        }

        // skip layer 0 because its input layer, no parameters
        for (int i = 1; i < nn.layers.length; i++) {
            Layer l = nn.getLayer(i);
            Logger.DebugLog("fetching param from layer: " + l.name, Logger.Role.APP, 0);
            String matrixName = l.name + "." + Model.MATRIX_PARAM;

            DMatrix matrix = nn.getMatrix(matrixName);
            if (matrix != null) {
                Matrix result = dataBus.fetchFromServer(matrixName, KeyCollection.ALL, KeyCollection.ALL);
                matrix.setLocalCache(result);
            }
        }
    }

}
