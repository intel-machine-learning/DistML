package com.intel.distml.util.store;

import com.intel.distml.util.*;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by yunlong on 12/8/15.
 */
public class IntArrayStore extends DataStore {

    public static final int VALUE_SIZE = 4;

    transient KeyCollection localRows;
    transient int[] localData;

    public KeyCollection rows() {
        return localRows;
    }
    public int rowSize() {
        return 1;
    }

    public void init(KeyCollection keys) {
        this.localRows = keys;
        localData = new int[(int)keys.size()];

        for (int i = 0; i < keys.size(); i++)
            localData[i] = 0;
    }

    public int indexOf(long key) {
        if (localRows instanceof KeyRange) {
            return (int) (key - ((KeyRange)localRows).firstKey);
        }
        else if (localRows instanceof KeyHash) {
            KeyHash hash = (KeyHash) localRows;
            return (int) ((key - hash.minKey) % hash.hashQuato);
        }

        throw new RuntimeException("Only KeyRange or KeyHash is allowed in server storage");
    }

    @Override
    public void writeAll(DataOutputStream os) throws IOException {
        for (int i = 0; i < localData.length; i++) {
            os.writeInt(localData[i]);
        }
    }

    @Override
    public void readAll(DataInputStream is) throws IOException {
        for (int i = 0; i < localData.length; i++) {
            localData[i] = is.readInt();
        }
    }

    @Override
    public void syncTo(DataOutputStream os, int fromRow, int toRow) throws IOException {
        for (int i = fromRow; i <= toRow; i++) {
            os.writeInt(localData[i]);
        }
    }

    @Override
    public void syncFrom(DataInputStream is, int fromRow, int toRow) throws IOException {
        for (int i = fromRow; i <= toRow; i++) {
            localData[i] = is.readInt();
        }
    }

    @Override
    public byte[] handleFetch(DataDesc format, KeyCollection rows) {

        KeyCollection keys = localRows.intersect(rows);
        int len = (int) ((format.keySize + VALUE_SIZE) * keys.size());
        byte[] buf = new byte[len];

        Iterator<Long> it = keys.iterator();
        int offset = 0;
        while(it.hasNext()) {
            long k = it.next();
            format.writeKey((Number) k, buf, offset);
            offset += format.keySize;
            format.writeValue(localData[indexOf(k)], buf, offset);
            offset += VALUE_SIZE;
        }

        return buf;
    }

    public void handlePush(DataDesc format, byte[] data) {

        int offset = 0;
        while (offset < data.length) {
            long key = format.readKey(data, offset).longValue();
            offset += format.keySize;

            int update = format.readInt(data, offset);
            offset += VALUE_SIZE;

            localData[indexOf(key)] += update;
            if (localData[indexOf(key)] < 0) {
                throw new IllegalStateException("invalid k counter: " + key + ", " + localData[indexOf(key)]);
            }

        }
    }

}
