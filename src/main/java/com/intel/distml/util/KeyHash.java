package com.intel.distml.util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;

/**
 * Created by yunlong on 12/11/14.
 */
public class KeyHash extends KeyCollection {

    public int hashQuato;
    public int hashIndex;

    public long minKey;
    public long maxKey;
    //public long totalKeyNum;

    private long first, last;

    KeyHash() {
        super(KeyCollection.TYPE_HASH);
    }

    public KeyHash(int hashQuato, int hashIndex, long minKey, long maxKey) {
        super(KeyCollection.TYPE_HASH);

        this.hashQuato = hashQuato;
        this.hashIndex = hashIndex;
        this.minKey = minKey;
        this.maxKey = maxKey;

        first = minKey - minKey%hashQuato + hashIndex;
        if (first < minKey) {
            first += hashQuato;
        }
        last = maxKey - maxKey%hashQuato + hashIndex;
        if (last > maxKey) {
            last -= hashQuato;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof KeyHash)) {
            return false;
        }

        KeyHash o = (KeyHash)obj;
        return (hashQuato == o.hashQuato) && (hashIndex == o.hashIndex) && (minKey == o.minKey) && (maxKey == o.maxKey);
    }

    @Override
    public int sizeAsBytes(DataDesc format) {
        return super.sizeAsBytes(format) + 8 + 4 * format.keySize;
    }

    @Override
    public void write(AbstractDataWriter out, DataDesc format) throws Exception {
        super.write(out, format);

        out.writeInt(hashQuato);
        out.writeInt(hashIndex);

        if (format.keyType == DataDesc.KEY_TYPE_INT) {
            out.writeInt((int)minKey);
            out.writeInt((int)maxKey);
            out.writeInt((int)first);
            out.writeInt((int)last);
        }
        else {
            out.writeLong(minKey);
            out.writeLong(maxKey);
            out.writeLong(first);
            out.writeLong(last);
        }
    }

    @Override
    public void read(AbstractDataReader in, DataDesc format) throws Exception {
        //super.read(in);
        hashQuato = in.readInt();
        hashIndex = in.readInt();
        if (format.keyType == DataDesc.KEY_TYPE_INT) {
            minKey = in.readInt();
            maxKey = in.readInt();
            first = in.readInt();
            last = in.readInt();
        }
        else {
            minKey = in.readLong();
            maxKey = in.readLong();
            first = in.readLong();
            last = in.readLong();
        }
    }


    public long size() {
        return (last - first) / hashQuato + 1;
        //return (totalKeyNum /hashQuato) + (((totalKeyNum % hashQuato) > hashIndex)? 1 : 0);
    }

    @Override
    public boolean contains(long key) {
        if ((key > last) || (key < first)) {
            //throw new RuntimeException("unexpected key: " + key + " >= " + totalKeyNum);
            return false;
        }

        //System.out.println("check contains: " + key);
        return key % hashQuato == hashIndex;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public String toString() {
        return "[KeyHash: quato=" + hashQuato + ", index=" + hashIndex + ", min=" + minKey + ", max=" + maxKey + "]";
    }

    @Override
    public KeyCollection intersect(KeyCollection keys) {

        if (keys.equals(KeyCollection.ALL)) {
            return this;
        }

        if (keys.equals(KeyCollection.EMPTY)) {
            return keys;
        }

        if (keys instanceof KeyRange) {
            KeyRange r = (KeyRange) keys;
            if ((r.firstKey <= first) && (r.lastKey >= last)) {
                return this;
            }

            KeyHash newKeys = new KeyHash(hashQuato, hashIndex, Math.max(minKey, r.firstKey), Math.min(maxKey, r.lastKey));
            return newKeys;
        }

        KeyList list = new KeyList();
        Iterator<Long> it = keys.iterator();
        while(it.hasNext()) {
            long key = it.next();
            if (contains(key)) {
                list.addKey(key);
            }
        }

        if (list.isEmpty()) {
            return KeyCollection.EMPTY;
        }

        return list;
    }


    @Override
    public Iterator<Long> iterator() {
        return new _Iterator(this);
    }

    static class _Iterator implements Iterator<Long> {

        long currentKey;
        KeyHash keys;

        public _Iterator(KeyHash keys) {
            this.keys = keys;
            this.currentKey = keys.first;
        }

        public boolean hasNext() {
            return currentKey <= keys.last;
        }

        public Long next() {
            long k = currentKey;
            currentKey += keys.hashQuato;
            return k;
        }

        public void remove() {
            throw new RuntimeException("Not supported.");
        }

    }
}
