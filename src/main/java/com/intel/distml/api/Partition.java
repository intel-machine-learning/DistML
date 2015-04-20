package com.intel.distml.api;

import com.intel.distml.util.KeyCollection;

import java.io.Serializable;

/**
 * Created by yunlong on 12/13/14.
 */
public class Partition implements Serializable {

    public int index; // TODO Remove this?

    public KeyCollection keys;

    public Partition() {}

    public Partition(KeyCollection keys) {
        this.keys = keys;
    }

    public KeyCollection intersect(Partition p) {
        if (p == null) {
            return keys;
        }

        return keys.intersect(p.keys);
    }
}
