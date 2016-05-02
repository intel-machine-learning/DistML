package com.intel.distml.api;

import com.intel.distml.util.DataDesc;
import com.intel.distml.util.KeyCollection;

import java.util.HashMap;

/**
 * Created by yunlong on 6/3/15.
 */
public interface DataBus {

    public byte[][] fetch(String matrixName, KeyCollection rowKeys, DataDesc format);

    public void push(String matrixName, DataDesc format, byte[][] data);

    public void disconnect();

    public void psAvailable(int index, String addr);
}
