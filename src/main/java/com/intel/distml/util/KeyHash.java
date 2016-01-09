package com.intel.distml.util;

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

    public KeyHash(int hashQuato, int hashIndex, long totalKeyNum) {
        this(hashQuato, hashIndex, 0L, totalKeyNum - 1);
    }

    public KeyHash(int hashQuato, int hashIndex, long minKey, long maxKey) {
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
