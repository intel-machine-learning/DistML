package com.intel.distml.util;

import java.util.Iterator;

/**
 * Created by yunlong on 12/11/14.
 */
public class KeyRange implements KeyCollection {

    public static final KeyRange Single = new KeyRange(0, 0);

    public long firstKey, lastKey;

    public KeyRange(long f, long l) {
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

    public KeyCollection[] linearSplit(int hostNum) {
        System.out.println("linearly partition key range [" + firstKey + ", " + lastKey + "] for " + hostNum + " hosts");
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
        System.out.println("hash partition key range [" + firstKey + ", " + lastKey + "] for " + hostNum + " hosts");
        KeyCollection[] sets = new KeyHash[hostNum];

        for (int i = 0; i < hostNum; i++) {
            sets[i] = new KeyHash(hostNum, i, lastKey+1);
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

        if (keys.equals(KeyCollection.ALL)) {
            return this;
        }

        if (keys.equals(KeyCollection.EMPTY)) {
            return keys;
        }

        if (keys instanceof  KeyRange) {
            KeyRange r = (KeyRange)keys;
            long min = Math.max(r.firstKey, firstKey);
            long max = Math.min(r.lastKey, lastKey);

            if (min > max) {
                return KeyCollection.EMPTY;
            }

            return  new KeyRange(min, max);
        }
        else if (keys instanceof KeyList) {
            return ((KeyList)keys).intersect(this);
        }

        //todo support non-range keys
//        else if (keys instanceof KeyHash) {
//
//        }
        throw new RuntimeException("Not supported.");
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

        @Override
        public boolean hasNext() {
            return index <= range.lastKey;
        }

        @Override
        public Long next() {
            return index++;
        }

        @Override
        public void remove() {
            throw new RuntimeException("Not supported.");
        }

    }
}
