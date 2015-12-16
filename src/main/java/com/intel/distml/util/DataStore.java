package com.intel.distml.util;

import com.intel.distml.api.DMatrix;
import com.intel.distml.util.store.DoubleArrayStore;

/**
 * Created by yunlong on 12/8/15.
 */
public abstract class DataStore {

    public abstract Object partialData(KeyCollection rows);

    public abstract void mergeUpdate(Object update);

}
