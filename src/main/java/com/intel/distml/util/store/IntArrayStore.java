package com.intel.distml.util.store;

import com.intel.distml.util.DataStore;
import com.intel.distml.util.KeyCollection;
import com.intel.distml.util.KeyHash;
import com.intel.distml.util.KeyRange;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by yunlong on 12/8/15.
 */
public class IntArrayStore extends DataStore {

    transient KeyCollection localRows;
    transient int[] localData;

    public void init(KeyCollection keys) {
        this.localRows = keys;
        localData = new int[(int)keys.size()];
    }

    public Object partialData(KeyCollection rows) {

        HashMap<Long, Integer> tmp = new HashMap<Long, Integer>();

        Iterator<Long> it = rows.iterator();
        while(it.hasNext()) {
            long k = it.next();
            if (localRows.contains(k)) {
                tmp.put(k, localData[indexOf(k)]);
            }
        }

        return tmp;
    }

    public int indexOf(long key) {
        if (localRows instanceof KeyRange) {
            return (int) (key - ((KeyRange)localRows).firstKey);
        }
        else if (localRows instanceof KeyHash) {
            KeyHash hash = (KeyHash) localRows;
            return (int) ((key - hash.minKey) % hash.hashQuato);
        }

        throw new RuntimeException("Only KeyRange or KeyHash is allowed in server storage");
    }

    public void mergeUpdate(Object obj) {

        HashMap<Long, Integer> update = (HashMap<Long, Integer>) obj;

        for(Map.Entry<Long, Integer> entry : update.entrySet()) {
            long k = entry.getKey();
            if (localRows.contains(k)) {
                int index = indexOf(k);
                int tmp = localData[index];
                localData[index] = tmp + entry.getValue();
            }
        }
    }
}
