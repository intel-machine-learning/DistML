package com.intel.distml.util;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Created by yunlong on 12/11/14.
 */
public abstract class KeyCollection implements Serializable {

    public static final KeyCollection EMPTY = new EMPTY_KEYS();
//    public static final KeyCollection SINGLE = new SINGLE_KEYS();
    public static final KeyCollection ALL = new ALL_KEYS();

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

    public static class EMPTY_KEYS extends KeyCollection {

        @Override
        public boolean equals(Object obj) {
            return (obj instanceof EMPTY_KEYS);
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
            throw new UnsupportedOperationException("This is an EMPTY_KEYS instance, not iterable.");
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

    public static class SINGLE_KEYS extends KeyCollection {

        @Override
        public boolean equals(Object obj) {
            return (obj instanceof SINGLE_KEYS);
        }

        @Override
        public boolean contains(long key) {
            throw new UnsupportedOperationException("This is an SINGLE_KEYS instance, not iterable.");
        }

        @Override
        public KeyCollection intersect(KeyCollection keys) {
            if (keys.isEmpty()) {
                return keys;
            }
            if (keys instanceof SINGLE_KEYS) {
                return this;
            }
            if (keys instanceof ALL_KEYS) {
                return this;
            }
            throw new UnsupportedOperationException("This is an SINGLE_KEYS instance, not iterable.");
        }

        @Override
        public Iterator<Long> iterator() {
            throw new UnsupportedOperationException("This is an SINGLE_KEYS instance, not iterable.");
        }

        @Override
        public boolean isEmpty() {
            return false;
        }
/*
        @Override
        public PartitionInfo partitionEqually(int hostNum) {
            throw new UnsupportedOperationException("This is an SINGLE_KEYS instance, not partitionable");
        }

        @Override
        public KeyCollection[] split(int hostNum) {
            throw new UnsupportedOperationException("This is an SINGLE_KEYS instance, not partitionable");
        }
*/
        @Override
        public long size() {
            return 1;
        }
    };

    public static class ALL_KEYS extends KeyCollection {

        @Override
        public boolean equals(Object obj) {
            return (obj instanceof ALL_KEYS);
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


