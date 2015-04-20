package com.intel.distml.util;

import java.util.HashSet;
import java.util.Iterator;

/**
 * Created by yunlong on 2/1/15.
 */
public class KeyList implements KeyCollection {

    public HashSet<Long> keys;

    public KeyList() {
        keys = new HashSet<Long>();
    }

    public int size() {
        return keys.size();
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
/*
    public PartitionInfo partitionEqually(int hostNum) {
        throw new UnsupportedOperationException("This method is not supported.");
    }

    @Override
    public KeyCollection[] split(int hostNum) {
        throw new UnsupportedOperationException("This method is not supported.");
    }
*/
}
