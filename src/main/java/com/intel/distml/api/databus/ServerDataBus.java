package com.intel.distml.api.databus;

import com.intel.distml.util.KeyCollection;
import com.intel.distml.util.Matrix;

/**
 * Created by yunlong on 2/2/15.
 */
public interface ServerDataBus {

    public Matrix fetchFromServer(String matrixName);

    public Matrix fetchFromServer(String matrixName, KeyCollection rowKeys);

    public Matrix fetchFromServer(String matrixName, KeyCollection rowKeys, KeyCollection colsKeys);

    public void pushUpdate(String matrixName, Matrix update);


}
