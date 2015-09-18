package com.intel.distml.model.lda;

import com.intel.distml.api.DMatrix;
import com.intel.distml.util.GeneralArray;
import com.intel.distml.util.KeyCollection;
import com.intel.distml.util.KeyRange;
import com.intel.distml.util.Matrix;
import com.intel.distml.util.primitive.IntArray;

/**
 * Created by ruixiang on 6/17/15.
 */
public class ParamTopic extends DMatrix {

    private int K;//topic number

    public ParamTopic(int _K) {
        super(DMatrix.FLAG_ON_SERVER | DMatrix.FLAG_PARAM, _K);
        K = _K;
    }

    @Override
    public void initOnServer(int psIndex, KeyCollection keys) {
        System.out.println("Init Param Topic on server index " + psIndex
                + " with key range from " + ((KeyRange) keys).firstKey + " to " + ((KeyRange) keys).lastKey);

        long first = ((KeyRange) keys).firstKey;
        long last = ((KeyRange) keys).lastKey;

        int[] topic = new int[(int) (last - first + 1)];
        for (int i = (int) first; i <= last; i++) {
            topic[(int) (i - first)] = 0;
        }

        setLocalCache(new Topic(topic, (KeyRange) (keys)));
    }

    @Override
    public void mergeUpdate(int serverIndex, Matrix update) {
        ((Topic) localCache).mergeUpdate(serverIndex, (IntArray) update);
    }

}
