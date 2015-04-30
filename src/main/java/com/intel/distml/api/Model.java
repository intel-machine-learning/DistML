package com.intel.distml.api;

import com.intel.distml.api.databus.DataBus;
import com.intel.distml.api.databus.MonitorDataBus;
import com.intel.distml.util.Matrix;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

/**
 * Created by yunlong on 12/13/14.
 */
public class Model implements Serializable {

    public static final String MATRIX_SAMPLE = "SAMPLE";
    public static final String MATRIX_PARAM = "PARAM";
    public static final String MATRIX_UPDATE = "UPDATE";
    public static final String MATRIX_DATA = "DATA";
    public static final String MATRIX_ERROR = "ERROR";
    public static final String MATRIX_DELTA = "DELTA";

    //public int psNum, workerGroupSize;
    public boolean serverReady;
    public boolean autoFetchParams;
    public boolean autoPushUpdates;

    public HashMap<String, DMatrix> dataMap;

    public Model() {

        serverReady = false;
        autoFetchParams = true;
        autoPushUpdates = true;
        dataMap = new HashMap<String, DMatrix>();

        registerMatrix(MATRIX_SAMPLE, new DMatrix(1));
    }

    public void registerMatrix(String name, DMatrix matrix) {
        if (dataMap.containsKey(name)) {
            throw new IllegalArgumentException("Matrix already exist: " + name);
        }
        dataMap.put(name, matrix);
    }

    public DMatrix getMatrix(String matrixName) {
        return dataMap.get(matrixName);
    }

    public Matrix getCache(String matrixName) {
        return dataMap.get(matrixName).localCache;
    }

    public void setCache(String matrixName, Matrix cache) {
        dataMap.get(matrixName).localCache = cache;
    }

    public void clearAllCache() {
        for (DMatrix m : dataMap.values()) {
            m.localCache = null;
        }
    }

    public void mergeUpdate(int serverIndex, String matrixName, Matrix update) {
        DMatrix m = getMatrix(matrixName);
        if (m != null) {
            m.mergeUpdate(serverIndex, update);
        }
    }

    public void autoPartition(int serverNum) {
        System.out.println("partitionParams");
        for (String matrixName: dataMap.keySet()) {
            System.out.println("check DMatrix: " + matrixName);
            DMatrix m = dataMap.get(matrixName);
            if (m.hasFlag(DMatrix.FLAG_ON_SERVER))
                continue;

            System.out.println("partition DMatrix: " + matrixName);
            m.partition(serverNum);
        }
    }

    public void preTraining(int workerIndex, DataBus dataBus) {
        if (!autoFetchParams) {
            return;
        }

        for (String matrixName: dataMap.keySet()) {
            DMatrix m = dataMap.get(matrixName);
            if (m.hasFlag(DMatrix.FLAG_PARAM))
                continue;

            PartitionInfo sp = m.serverPartitions();
            PartitionInfo wp = m.workerPartitions();

            if ((wp == null) || (wp.type == PartitionInfo.Type.COPIED)) {
                Matrix params = dataBus.fetchFromServer(matrixName);
                m.setLocalCache(params);
            } else {
                Partition p = wp.getPartition(workerIndex);
                Matrix params = dataBus.fetchFromServer(matrixName, p.keys);
                m.setLocalCache(params);
            }
        }
    }

    public void postTraining(int workerIndex, DataBus dataBus) {
        // post updates
        if (!autoPushUpdates) {
            return;
        }

        for (String matrixName: dataMap.keySet()) {
            DMatrix m = dataMap.get(matrixName);
            if (!(m instanceof DParam))
                continue;

            dataBus.pushUpdate(matrixName, ((DParam)m).update);
        }
    }

    public void compute(Matrix sample, int workerIndex, DataBus dataBus) {

    }

    public void progress(long totalSamples, long progress, MonitorDataBus dataBus) {

    }

    public void variableChanged(String name, Object value) {

    }

    /**
     * If applications uses non-matrix sample, this method should be override
     * to make it usable.
     *
     * todo: move this method out of api.
     *
     * @param samples

     * @return
     */
    public Matrix transformSamples(List<Object> samples) {
        return (Matrix) samples.get(0);
    }
}
