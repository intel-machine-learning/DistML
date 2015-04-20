package com.intel.distml.util;

/**
 * Created by yunlong on 12/13/14.
 */
public interface LocalStore<K, V> {

    void put(K key, V value);

    V get(K key);

    void clear();
}

