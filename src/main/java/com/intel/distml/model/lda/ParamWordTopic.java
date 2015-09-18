package com.intel.distml.model.lda;

import com.intel.distml.api.DMatrix;
import com.intel.distml.api.Partition;
import com.intel.distml.api.PartitionInfo;
import com.intel.distml.util.*;

import java.util.Iterator;

/**
 * Created by ruixiang on 5/18/15.
 */
public class ParamWordTopic extends DMatrix {


    private int K;//topic number

    public ParamWordTopic(int dicSize, int _K) {
        super(DMatrix.FLAG_ON_SERVER | DMatrix.FLAG_PARAM, dicSize);
        K = _K;
    }

    @Override
    public void initOnServer(int psIndex, KeyCollection keys) {
        System.out.println("init the word-topic matrix on server Index " + psIndex
                + " with key range from " + ((KeyRange) keys).firstKey + " to " + ((KeyRange) keys).lastKey);

        long first = ((KeyRange) keys).firstKey;
        long last = ((KeyRange) keys).lastKey;

        int[][] wordTopic = new int[(int) (last - first + 1)][K];
        for (int i = (int) first; i <= last; i++)
            for (int j = 0; j < K; j++) {
                wordTopic[i - (int) first][j] = new Integer(0);
            }

        setLocalCache(new WordTopic(wordTopic, (KeyRange) keys, new KeyRange(0, K - 1)));
    }

    @Override
    public void mergeUpdate(int serverIndex, Matrix update) {
        ((WordTopic) localCache).mergeUpdate(serverIndex, update);
    }


}
