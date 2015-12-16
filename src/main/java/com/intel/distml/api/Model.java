package com.intel.distml.api;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Created by yunlong on 12/13/14.
 */
public class Model implements Serializable {

    public HashMap<String, DMatrix> dataMap;

    public String monitorPath;

    public int psCount;
    public boolean psReady;

    public Model() {
        dataMap = new HashMap<String, DMatrix>();
        psReady = false;
    }

    public void registerMatrix(String name, DMatrix matrix) {
        if (dataMap.containsKey(name)) {
            throw new IllegalArgumentException("Matrix already exist: " + name);
        }
        matrix.name = name;
        dataMap.put(name, matrix);
    }

    public DMatrix getMatrix(String matrixName) {
        return dataMap.get(matrixName);
    }

    public void autoPartition(int serverNum) {
        this.psCount = serverNum;

        for (String matrixName: dataMap.keySet()) {
            DMatrix m = dataMap.get(matrixName);
            m.partition(serverNum);
        }
    }

}
