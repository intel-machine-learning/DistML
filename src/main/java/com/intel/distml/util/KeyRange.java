package com.intel.distml.util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;

/**
 * Created by yunlong on 12/11/14.
 */
public class KeyRange extends KeyCollection {

    public static final KeyRange Single = new KeyRange(0, 0);

    public long firstKey, lastKey;

    KeyRange() {
        super(KeyCollection.TYPE_RANGE);
    }

    public KeyRange(long f, long l) {
        super(KeyCollection.TYPE_RANGE);
        firstKey = f;
        lastKey = l;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof KeyRange)) {
            return false;
        }

        KeyRange range = (KeyRange)obj;
        return (firstKey == range.firstKey) && (lastKey == range.lastKey);
    }

    @Override
    public int sizeAsBytes(DataDesc format) {
        return super.sizeAsBytes(format) + 2 * format.keySize;
    }

    @Override
    public void write(AbstractDataWriter out, DataDesc format) throws Exception {
        super.write(out, format);

        if (format.keyType == DataDesc.KEY_TYPE_INT) {
            out.writeInt((int)firstKey);
            out.writeInt((int)lastKey);
        }
        else {
            out.writeLong(firstKey);
            out.writeLong(lastKey);
        }
    }

    @Override
    public void read(AbstractDataReader in, DataDesc format) throws Exception {
        if (format.keyType == DataDesc.KEY_TYPE_INT) {
            firstKey = in.readInt();
            lastKey = in.readInt();
        }
        else {
            firstKey = in.readLong();
            lastKey = in.readLong();
        }
    }

    public KeyCollection[] linearSplit(int hostNum) {
        KeyCollection[] sets = new KeyRange[hostNum];

        long start = firstKey;
        long step = (lastKey - firstKey + hostNum) / hostNum;
        for (int i = 0; i < hostNum; i++) {
            long end = Math.min(start + step - 1, lastKey);
            sets[i] = new KeyRange(start, end);
            start += step;
        }

        return sets;
    }

    public KeyCollection[] hashSplit(int hostNum) {
        KeyCollection[] sets = new KeyHash[hostNum];

        for (int i = 0; i < hostNum; i++) {
            sets[i] = new KeyHash(hostNum, i, firstKey, lastKey);
        }

        return sets;
    }

    public long size() {
        return lastKey - firstKey + 1;
    }

    @Override
    public boolean contains(long key) {
        return (key >= firstKey) && (key <= lastKey);
    }

    @Override
    public boolean isEmpty() {
        return firstKey > lastKey;
    }

    public boolean containsAll(KeyCollection keys) {
        KeyRange keyRange = (KeyRange)keys;
        return (keyRange.lastKey >= firstKey && keyRange.firstKey <= lastKey);
    }

    @Override
    public String toString() {
        return "[" + firstKey + ", " + lastKey + "]";
    }

    public KeyRange FetchSame(KeyRange kr) {
        long NewFirst = kr.firstKey > this.firstKey ? kr.firstKey : this.firstKey;
        long NewLast = kr.lastKey < this.lastKey ? kr.lastKey : this.lastKey;
        if (NewFirst > NewLast) return null;
        return new KeyRange(NewFirst, NewLast);
    }

    @Override
    public KeyCollection intersect(KeyCollection keys) {

        if (keys instanceof  KeyRange) {
            KeyRange r = (KeyRange)keys;
            long min = Math.max(r.firstKey, firstKey);
            long max = Math.min(r.lastKey, lastKey);

            if (min > max) {
                return KeyCollection.EMPTY;
            }

            return  new KeyRange(min, max);
        }

        if (keys instanceof KeyHash) {
            // warning: hope KeyHash don't call back
            return keys.intersect(this);
        }

        return super.intersect(keys);
    }

    public boolean mergeFrom(KeyRange keys) {
        if ((keys.firstKey > lastKey) && (keys.lastKey < firstKey)) {
            return false;
        }

        firstKey = Math.min(keys.firstKey, firstKey);
        lastKey = Math.max(keys.lastKey, lastKey);
        return true;
    }


    @Override
    public Iterator<Long> iterator() {
        return new _Iterator(this);
    }

    static class _Iterator implements Iterator<Long> {

        long index;
        KeyRange range;

        public _Iterator(KeyRange range) {
            this.range = range;
            this.index = range.firstKey;
        }

        public boolean hasNext() {
            return index <= range.lastKey;
        }

        public Long next() {
            return index++;
        }

        public void remove() {
            throw new RuntimeException("Not supported.");
        }

    }
}
