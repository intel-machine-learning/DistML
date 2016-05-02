package com.intel.distml.util.store;

import com.intel.distml.util.*;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;

/**
 * Created by yunlong on 12/8/15.
 */
public class FloatArrayStore extends DataStore {

    public static final int VALUE_SIZE = 8;

    transient KeyCollection localRows;
    transient float[] localData;

    public KeyCollection rows() {
        return localRows;
    }
    public int rowSize() {
        return 1;
    }

    public void init(KeyCollection keys) {
        this.localRows = keys;
        localData = new float[(int)keys.size()];
        for (int i = 0; i < keys.size(); i++)
            localData[i] = 0.0f;
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

    public long keyOf(int index) {
        if (localRows instanceof KeyRange) {
            return ((KeyRange)localRows).firstKey + index;
        }
        else if (localRows instanceof KeyHash) {
            KeyHash hash = (KeyHash) localRows;
            return hash.minKey + index * hash.hashQuato;
        }

        throw new RuntimeException("Only KeyRange or KeyHash is allowed in server storage");
    }

    @Override
    public void writeAll(DataOutputStream os) throws IOException {
        System.out.println("saving to: " + localData.length);
        for (int i = 0; i < localData.length; i++) {
            os.writeFloat(localData[i]);
        }
    }

    @Override
    public void readAll(DataInputStream is) throws IOException {
        for (int i = 0; i < localData.length; i++) {
            localData[i] = is.readFloat();
        }
    }

    @Override
    public void syncTo(DataOutputStream os, int fromRow, int toRow) throws IOException {
        for (int i = fromRow; i <= toRow; i++) {
            os.writeFloat(localData[i]);
        }
    }

    @Override
    public void syncFrom(DataInputStream is, int fromRow, int toRow) throws IOException {
        for (int i = fromRow; i <= toRow; i++) {
            localData[i] = is.readFloat();
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

            float value = localData[indexOf(k)];
            format.writeValue(value, buf, offset);
            offset += VALUE_SIZE;
        }
        return buf;
    }

    public void handlePush(DataDesc format, byte[] data) {

        int offset = 0;
        while (offset < data.length) {
            long key = format.readKey(data, offset).longValue();
            offset += format.keySize;

            float update = format.readFloat(data, offset);
            offset += VALUE_SIZE;

            localData[indexOf(key)] += update;
        }
    }

    public Iter iter() {
        return new Iter();
    }

    public class Iter {

        int p;

        public Iter() {
            p = -1;
        }

        public boolean hasNext() {
            return p < localData.length - 1;
        }

        public long key() {
            return keyOf(p);
        }

        public float value() {
            return localData[p];
        }

        public boolean next() {
            p++;
            return p < localData.length;
        }
    }

}
