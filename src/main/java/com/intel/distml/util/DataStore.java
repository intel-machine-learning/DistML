package com.intel.distml.util;

import com.intel.distml.api.DMatrix;
import com.intel.distml.api.Model;
import com.intel.distml.util.store.DoubleArrayStore;
import com.intel.distml.util.store.DoubleMatrixStore;
import com.intel.distml.util.store.IntArrayStore;
import com.intel.distml.util.store.IntMatrixStore;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by yunlong on 12/8/15.
 */
public abstract class DataStore {

    public abstract byte[] handleFetch(DataDesc format, KeyCollection rows);

    public abstract void handlePush(DataDesc format, byte[] data);

    public abstract void writeAll(DataOutputStream os) throws IOException;

    public abstract void readAll(DataInputStream is) throws IOException;

    public static HashMap<String, DataStore> createStores(Model model, int serverIndex) {
        HashMap<String, DataStore> stores = new HashMap<String, DataStore>();
        for (Map.Entry<String, DMatrix> m : model.dataMap.entrySet()) {
            if (m.getKey().equals("SAMPLE")) continue;

            stores.put(m.getKey(), DataStore.createStore(serverIndex, m.getValue()));
        }

        return stores;
    }

    public static DataStore createStore(int serverIndex, DMatrix matrix) {
        DataDesc format = matrix.getFormat();
        if (format.dataType == DataDesc.DATA_TYPE_ARRAY) {
            if (format.valueType == DataDesc.ELEMENT_TYPE_INT) {
                IntArrayStore store = new IntArrayStore();
                store.init(matrix.partitions[serverIndex]);
                return store;
            }
            else if (format.valueType == DataDesc.ELEMENT_TYPE_DOUBLE) {
                DoubleArrayStore store = new DoubleArrayStore();
                store.init(matrix.partitions[serverIndex]);
                return store;
            }
        }
        else {
            if (format.valueType == DataDesc.ELEMENT_TYPE_INT) {
                IntMatrixStore store = new IntMatrixStore();
                store.init(matrix.partitions[serverIndex], (int) matrix.getColKeys().size());
                return store;
            } else if (format.valueType == DataDesc.ELEMENT_TYPE_DOUBLE) {
                DoubleMatrixStore store = new DoubleMatrixStore();
                store.init(matrix.partitions[serverIndex], (int) matrix.getColKeys().size());
                return store;
            }
        }

        throw new IllegalArgumentException("Unrecognized matrix type: " + matrix.getClass().getName());
    }

}
