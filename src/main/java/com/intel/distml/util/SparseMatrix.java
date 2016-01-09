package com.intel.distml.util;

import com.intel.distml.api.DMatrix;
import com.intel.distml.api.Session;

import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by yunlong on 12/31/15.
 */
public abstract class SparseMatrix<K, V> extends DMatrix {

    protected KeyRange colKeys;

    public SparseMatrix(long dim, int cols, int keyType, int valueType) {
        super(dim);
        format = new DataDesc(DataDesc.DATA_TYPE_MATRIX, keyType, valueType);
        colKeys = new KeyRange(0, cols-1);
    }

    public SparseMatrix(long dim, int cols, int keyType, int valueType,
                        boolean denseColumn) {
        super(dim);
        format = new DataDesc(DataDesc.DATA_TYPE_MATRIX, keyType, valueType, false, denseColumn);
        colKeys = new KeyRange(0, cols-1);
    }

    public KeyCollection getColKeys() {
        return colKeys;
    }


    public HashMap<K, V[]> fetch(KeyCollection rows, Session session) {
        HashMap<K, V[]> result = new HashMap<K, V[]>();
        byte[][] data = session.dataBus.fetch(name, rows, format);
        for (byte[] obj : data) {
            HashMap<K, V[]> m = readMap(obj);
            result.putAll(m);
        }

        return result;
    }

    public void push(HashMap<K, V[]> data, Session session) {
        byte[][] bufs = new byte[partitions.length][];
        for (int i = 0; i < partitions.length; ++i) {
            KeyCollection p = partitions[i];
            HashMap<K, V[]> m = new HashMap<K, V[]>();
            for (K key : data.keySet()) {
                long k = (key instanceof Integer)? ((Integer)key).longValue() : ((Long)key).longValue();
                if (p.contains(k))
                    m.put(key, data.get(key));
            }
            bufs[i] = writeMap(m);
        }

        session.dataBus.push(name, format, bufs);
    }

    private HashMap<K, V[]> readMap(byte[] buf) {
        HashMap<K, V[]> data = new HashMap<K, V[]>();

        int offset = 0;
        while (offset < buf.length) {
            K key = (K) format.readKey(buf, offset);
            offset += format.keySize;

            V[] values = createValueArray((int) getColKeys().size());
            if (format.denseColumn) {
                for (int i = 0; i < getColKeys().size(); i++) {
                    values[i] = (V) format.readValue(buf, offset);
                    offset += format.valueSize;
                }
            }
            else {
                int count = format.readInt(buf, offset);
                offset += 4;

                for (int i = 0; i < count; i++) {
                    int index = format.readInt(buf, offset);
                    offset += 4;
                    V value = (V) format.readValue(buf, offset);
                    offset += format.valueSize;

                    values[index] = value;
                }
            }
            data.put(key, values);
        }

        return data;
    }

    private byte[] writeMap(HashMap<K, V[]> data) {

        byte[] buf;
        if (format.denseColumn) {
            int len = (int) (format.valueSize * data.size() * colKeys.size());
            buf = new byte[format.keySize * data.size() + len];
        }
        else {
            int nzcount = 0;
            for (V[] values : data.values()) {
                for (V value : values) {
                    if (!isZero(value)) {
                        nzcount++;
                    }
                }
            }
            int len = (int) ((format.valueSize + 4) * nzcount);
            buf = new byte[format.keySize * data.size() + len];
        }

        int offset = 0;
        for (Map.Entry<K, V[]> entry : data.entrySet()) {
            format.writeKey((Number)entry.getKey(), buf, offset);
            offset += format.keySize;
            V[] values = entry.getValue();
            if (format.denseColumn) {
                for (int i = 0; i < getColKeys().size(); i++) {
                    format.writeValue(values[i], buf, offset);
                    offset += format.valueSize;
                }
            }
            else {
                int counterIndex = offset;
                offset += 4;

                int counter = 0;
                for (int i = 0; i < values.length; i++) {
                    V value = values[i];
                    if (!isZero(value)) {
                        format.write(i, buf, offset);
                        offset += 4;
                        format.writeValue(value, buf, offset);
                        offset += format.valueSize;
                    }

                    counter++;
                }
                format.write(counter, buf, counterIndex);
            }
        }

        return buf;
    }

    abstract protected boolean isZero(V value);
    abstract protected V[] createValueArray(int size);
}
