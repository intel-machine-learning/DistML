package com.intel.distml.util;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Created by yunlong on 12/11/14.
 */
public interface KeyCollection extends Serializable {

    public static final KeyCollection EMPTY = new EMPTY_KEYS();
//    public static final KeyCollection SINGLE = new SINGLE_KEYS();
    public static final KeyCollection ALL = new ALL_KEYS();

    boolean contains(long key);

    KeyCollection intersect(KeyCollection keys);

    Iterator<Long> iterator();

    boolean isEmpty();

    int size();
/*
    public PartitionInfo partitionEqually(int partitionNum);

    public KeyCollection[] split(int hostNum);
*/
    static class EMPTY_KEYS implements KeyCollection {

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
        public int size() {
            return 0;
        }
    };

    static class SINGLE_KEYS implements KeyCollection {

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
        public int size() {
            return 1;
        }
    };

    static class ALL_KEYS implements KeyCollection {

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
        public int size() {
            throw new UnsupportedOperationException("This is an ALL_KEYS instance, size unknown");
        }
    };
}


