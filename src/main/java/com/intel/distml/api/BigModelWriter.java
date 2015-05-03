package com.intel.distml.api;

import com.intel.distml.api.DMatrix;
import com.intel.distml.api.Model;
import com.intel.distml.api.ModelWriter;
import com.intel.distml.api.databus.ServerDataBus;
import com.intel.distml.util.KeyRange;
import com.intel.distml.util.Matrix;

/**
 * Created by yunlong on 4/29/15.
 */
public class BigModelWriter implements ModelWriter {

    private static final int MAX_FETCH_SIZE = 10240000;   // 10M

    private int maxFetchRows;

    public BigModelWriter(int estimatedRowSize) {
        maxFetchRows = 10240000 / estimatedRowSize;
    }

    @Override
    public void writeModel (Model model, ServerDataBus dataBus) {

        for (String matrixName : model.dataMap.keySet()) {
            DMatrix m = model.dataMap.get (matrixName);
            if (m.hasFlag(DMatrix.FLAG_PARAM)) {
                long size = m.getRowKeys().size();
                System.out.println("total param: " + size);

                m.setLocalCache(null);
                long start = 0L;
                while(start < size-1) {
                    long end = Math.min(start + maxFetchRows, size) - 1;
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
