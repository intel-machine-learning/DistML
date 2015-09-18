package com.intel.distml.model.lda;

import com.intel.distml.util.GeneralArray;
import com.intel.distml.util.KeyCollection;
import com.intel.distml.util.KeyRange;
import com.intel.distml.util.primitive.IntArray;

/**
 * Created by ruixiang on 7/8/15.
 */
public class Topic extends IntArray {
    public Topic(int[] _topic, KeyRange keys) {
        super(_topic, keys);
    }

    public Topic() {

    }

    public void mergeUpdate(int serverIndex, IntArray t) {
        long first = ((KeyRange) rowKeys).firstKey;
        long last = ((KeyRange) rowKeys).lastKey;
        long keySize = last - first + 1;
        System.out.println("this range:" + first + "-" + last);
        System.out.println("push range:" + ((KeyRange) t.rowKeys).firstKey + "-" + ((KeyRange) t.rowKeys).lastKey);

        for (int i = (int) first; i <= last; i++) {
            this.values[i - (int) keySize * serverIndex] = this.values[i - (int) keySize * serverIndex] + t.values[i - (int) keySize * serverIndex];
        }
    }
}