package com.intel.distml.util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

/**
 * Created by yunlong on 2/1/15.
 */
public class KeyList extends KeyCollection {

    public HashSet<Long> keys;

    public KeyList() {
        super(KeyCollection.TYPE_LIST);
        keys = new HashSet<Long>();
    }

    public long size() {
        return keys.size();
    }

    @Override
    public int sizeAsBytes(DataDesc format) {
        return super.sizeAsBytes(format) + 4 + keys.size() * format.keySize;
    }

    @Override
    public void write(AbstractDataWriter out, DataDesc format) throws Exception {
        super.write(out, format);

        out.writeInt(keys.size());
        if (format.keyType == DataDesc.KEY_TYPE_INT) {
            for (long key : keys)
                out.writeInt((int)key);
        }
        else {
            for (long key : keys)
                out.writeLong(key);
        }
    }

    @Override
    public void read(AbstractDataReader in, DataDesc format) throws Exception {

        int count = in.readInt();
        if (format.keyType == DataDesc.KEY_TYPE_INT) {
            for (int i = 0; i < count; i++)
                keys.add(new Long(in.readInt()));
        }
        else {
            for (int i = 0; i < count; i++)
                keys.add(in.readLong());
        }
    }

    public void addKey(long k) {
        keys.add(k);
    }

    @Override
    public boolean contains(long key) {
        return keys.contains(key);
    }

    @Override
    public boolean isEmpty() {
        return keys.isEmpty();
    }

    @Override
    public KeyCollection intersect(KeyCollection keys) {
        KeyList list = new KeyList();

        Iterator<Long> it = keys.iterator();
        while(it.hasNext()) {
            long k = it.next();
            if (contains(k)) {
                list.keys.add(k);
            }
        }

        return list;
    }

    @Override
    public Iterator<Long> iterator() {
        return keys.iterator();
    }

    @Override
    public String toString() {
        String s =  "[KeyList: size=" + keys.size();
        if (keys.size() > 0) {
            s += " first=" + keys.iterator().next();
        }
        s += "]";

        return s;
    }
}
