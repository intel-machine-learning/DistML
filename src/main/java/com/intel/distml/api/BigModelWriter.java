package com.intel.distml.api;

import com.intel.distml.api.DMatrix;
import com.intel.distml.api.Model;
import com.intel.distml.api.ModelWriter;
import com.intel.distml.api.databus.ServerDataBus;
import com.intel.distml.util.KeyRange;
import com.intel.distml.util.Matrix;

import java.util.HashMap;

/**
 * Created by yunlong on 4/29/15.
 */
public class BigModelWriter implements ModelWriter {

    private static final int MAX_FETCH_SIZE = 204800000;   // 200M

    protected int maxFetchRows;

    protected HashMap<String, Integer> fetchBatchSizes;

    public BigModelWriter() {
        this(10);
    }

    public BigModelWriter(int estimatedRowSize) {
        maxFetchRows = MAX_FETCH_SIZE / estimatedRowSize;
        fetchBatchSizes = new HashMap<String, Integer>();
    }

    public void setParamRowSize(String matrixName, int rowSize) {
        fetchBatchSizes.put(matrixName, MAX_FETCH_SIZE/rowSize);
    }

    @Override
    public void writeModel (Model model, ServerDataBus dataBus) {

        for (String matrixName : model.dataMap.keySet()) {

            int batchSize = maxFetchRows;
            if (fetchBatchSizes.containsKey(matrixName)) {
                batchSize = fetchBatchSizes.get(matrixName).intValue();
            }
            DMatrix m = model.dataMap.get (matrixName);
            if (m.hasFlag(DMatrix.FLAG_PARAM)) {
                long size = m.getRowKeys().size();
                System.out.println("total param: " + size);

                m.setLocalCache(null);
                long start = 0L;
                while(start < size-1) {
                    long end = Math.min(start + batchSize, size) - 1;
                    KeyRange range = new KeyRange(start, end);
                    System.out.println("fetch param: " + range);
                    Matrix result = dataBus.fetchFromServer (matrixName, range);
                    if (m.localCache == null) {
                        m.setLocalCache (result);
                    }
                    else {
                        System.out.println("merge to local cache.");
                        m.localCache.mergeMatrix(result);
                    }
                    System.out.println("fetch done: " + m.localCache);
                    start = end + 1;
                }
                System.out.println("fetched param: " + matrixName + ", size=" + m.localCache.getRowKeys().size());
            }
        }
    }
}
