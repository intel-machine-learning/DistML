package com.intel.distml.api;

import com.intel.distml.api.databus.ServerDataBus;
import com.intel.distml.util.Matrix;

/**
 * Created by yunlong on 2/1/15.
 */
public class DefaultModelWriter implements ModelWriter {

    public void writeModel(Model model, ServerDataBus dataBus) {

        // skip layer 0 because its input layer, no parameters
        for (String matrixName: model.dataMap.keySet()) {
            DMatrix m = model.dataMap.get(matrixName);
            if (m.type != DMatrix.TYPE_PARAM)
                continue;

            Matrix result = dataBus.fetchFromServer(matrixName);
            m.setLocalCache(result);
        }
    }

}
