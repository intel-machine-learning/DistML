package com.intel.distml.util;

import java.util.HashSet;
import java.util.Iterator;

/**
 * Created by yunlong on 2/1/15.
 */
public class KeyList extends KeyCollection {

    public HashSet<Long> keys;

    public KeyList() {
        keys = new HashSet<Long>();
    }

    public long size() {
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
