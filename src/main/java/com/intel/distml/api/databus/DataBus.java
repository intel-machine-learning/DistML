package com.intel.distml.api.databus;

import com.intel.distml.util.KeyCollection;
import com.intel.distml.util.Matrix;

/**
 * Created by lantao on 1/18/15.
 */
public interface DataBus extends ServerDataBus {

    public Matrix fetchFromWorker(String matrixName);

    public Matrix fetchFromWorker(String matrixName, KeyCollection rowKeys);

    public Matrix fetchFromWorker(String matrixName, KeyCollection rowKeys, KeyCollection colsKeys);

    public void barrier();
}
