package com.intel.distml.util;

import com.intel.distml.api.DMatrix;
import com.intel.distml.api.Session;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yunlong on 12/31/15.
 */
public abstract class SparseArray<K, V> extends DMatrix {

    public SparseArray(long dim, int keyType, int valueType) {
        super(dim);
        format = new DataDesc(DataDesc.DATA_TYPE_ARRAY, keyType, valueType);
    }

    public HashMap<K, V> fetch(KeyCollection rows, Session session) {
        HashMap<K, V> result = new HashMap<K, V>();
        byte[][] data = session.dataBus.fetch(name, rows, format);
        for (byte[] obj : data) {
            HashMap<K, V> m = readMap(obj);
            result.putAll(m);
        }

        return result;
    }

    public void push(HashMap<K, V> data, Session session) {
        byte[][] bufs = new byte[partitions.length][];
        for (int i = 0; i < partitions.length; ++i) {
            KeyCollection p = partitions[i];
            HashMap<K, V> m = new HashMap<K, V>();
            for (K key : data.keySet()) {
                long k = (key instanceof Integer)? ((Integer)key).longValue() : ((Long)key).longValue();
                if (p.contains(k))
                    m.put(key, data.get(key));
            }
            bufs[i] = writeMap(m);
        }

        session.dataBus.push(name, format, bufs);
    }

    private HashMap<K, V> readMap(byte[] buf) {
        HashMap<K, V> data = new HashMap<K, V>();

        int offset = 0;
        while (offset < buf.length) {
            K key = (K) format.readKey(buf, offset);
            offset += format.keySize;

            V value = (V) format.readValue(buf, offset);
            offset += format.valueSize;

            data.put(key, value);
        }

        return data;
    }

    private byte[] writeMap(HashMap<K, V> data) {
        int recordLen = format.keySize + format.valueSize;
        byte[] buf = new byte[recordLen * data.size()];

        int offset = 0;
        for (Map.Entry<K, V> entry : data.entrySet()) {
            format.writeKey((Number) entry.getKey(), buf, offset);
            offset += format.keySize;

            offset = format.writeValue(entry.getValue(), buf, offset);
        }

        return buf;
    }

}
