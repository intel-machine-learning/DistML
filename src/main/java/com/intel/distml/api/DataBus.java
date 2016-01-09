package com.intel.distml.api;

import com.intel.distml.util.DataDesc;
import com.intel.distml.util.KeyCollection;

import java.util.HashMap;

/**
 * Created by yunlong on 6/3/15.
 */
public interface DataBus {

//    public <T> HashMap<Long, T> fetch(String matrixName, KeyCollection rowKeys, KeyCollection colsKeys);
//
//    public <T> void push(String matrixName, HashMap<Long, T> updates);
//
//    public <T> void push(String matrixName, T[] updates);

    public byte[][] fetch(String matrixName, KeyCollection rowKeys, DataDesc format);

    public void push(String matrixName, DataDesc format, byte[][] data);

    public void disconnect();
}
