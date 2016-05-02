package com.intel.distml.util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;

/**
 * Created by yunlong on 12/11/14.
 */
public abstract class KeyCollection implements Serializable {

    public static final int TYPE_ALL    = 0;
    public static final int TYPE_EMPTY  = 1;
    public static final int TYPE_RANGE  = 2;
    public static final int TYPE_LIST   = 3;
    public static final int TYPE_HASH   = 4;

    public static final KeyCollection EMPTY = new EmptyKeys();
    public static final KeyCollection SINGLE = new KeyRange(0, 0);
    public static final KeyCollection ALL = new AllKeys();

    public int type;

    public KeyCollection(int type) {
        this.type = type;
    }

    public int sizeAsBytes(DataDesc format) {
        return 4;
    }

    public void write(AbstractDataWriter out, DataDesc format) throws Exception {
        out.writeInt(type);
    }

    public void read(AbstractDataReader in, DataDesc format) throws Exception {
    }

    public static KeyCollection readKeyCollection(AbstractDataReader in, DataDesc format) throws Exception {
        in.waitUntil(4);
        int type = in.readInt();
        KeyCollection ks;
        switch(type) {
            case TYPE_ALL:
                return ALL;
            case TYPE_EMPTY:
                return EMPTY;

            case TYPE_LIST:
                ks = new KeyList();
                break;

            case TYPE_RANGE:
                ks = new KeyRange();
                break;

            default:
                ks = new KeyHash();
                break;

        }

        in.waitUntil(ks.sizeAsBytes(format));
        ks.read(in, format);

        return ks;
    }

    public abstract boolean contains(long key);

    public abstract Iterator<Long> iterator();

    public abstract boolean isEmpty();

    public abstract long size();

    public KeyCollection intersect(KeyCollection keys) {

        if (keys.equals(KeyCollection.ALL)) {
            return this;
        }

        if (keys.equals(KeyCollection.EMPTY)) {
            return keys;
        }

        KeyList result = new KeyList();

        Iterator<Long> it = keys.iterator();
        while(it.hasNext()) {
            long key = it.next();
            if (contains(key)) {
                result.addKey(key);
            }
        }

        return result;
    }

    public static class EmptyKeys extends KeyCollection {

        public EmptyKeys() {
            super(TYPE_EMPTY);
        }

        @Override
        public boolean equals(Object obj) {
            return (obj instanceof EmptyKeys);
        }

        @Override
        public boolean contains(long key) {
            return false;
        }

        @Override
        public KeyCollection intersect(KeyCollection keys) {
            return this;
        }

        @Override
        public Iterator<Long> iterator() {
            return new Iterator<Long>() {
                public boolean hasNext() { return false; }
                public Long next() { return -1L; }
                public void remove() {  }
            };
        }

        @Override
        public boolean isEmpty() {
            return true;
        }
/*
        @Override
        public PartitionInfo partitionEqually(int hostNum) {
            throw new UnsupportedOperationException("This is an EMPTY_KEYS instance, not partitionable");
        }

        @Override
        public KeyCollection[] split(int hostNum) {
            throw new UnsupportedOperationException("This is an EMPTY_KEYS instance, not partitionable");
        }
*/
        @Override
        public long size() {
            return 0;
        }
    };

    public static class AllKeys extends KeyCollection {

        public AllKeys() {
            super(TYPE_ALL);
        }


        @Override
        public boolean equals(Object obj) {
            return (obj instanceof AllKeys);
        }

        @Override
        public boolean contains(long key) {
            return true;
        }

        @Override
        public KeyCollection intersect(KeyCollection keys) {
            return keys;
        }

        @Override
        public Iterator<Long> iterator() {
            throw new UnsupportedOperationException("This is an ALL_KEYS instance, not iterable.");
        }

        @Override
        public boolean isEmpty() {
            return false;
        }
/*
        @Override
        public PartitionInfo partitionEqually(int hostNum) {
            throw new UnsupportedOperationException("This is an ALL_KEYS instance, not partitionable");
        }

        @Override
        public KeyCollection[] split(int hostNum) {
            throw new UnsupportedOperationException("This is an ALL_KEYS instance, not partitionable");
        }
*/
        @Override
        public long size() {
            throw new UnsupportedOperationException("This is an ALL_KEYS instance, size unknown");
        }
    };
}


