package com.intel.distml.model.lda;

import com.intel.distml.util.KeyCollection;
import com.intel.distml.util.Matrix;

/**
 * Created by ruixiang on 5/20/15.
 */
public class LDADataMatrix extends Matrix {
    public int[] topics;
    public int[] words;
    int[] nDocTopic;//spec:maybe [][],but doc is only one.
    public LDADataMatrix(int[] _topic,int[] _words,int[] _docTopic){
        topics=_topic;
        words=_words;
        nDocTopic=_docTopic;
    }

    @Override
    public KeyCollection getRowKeys() {
        return null;
    }

    @Override
    public KeyCollection getColKeys() {
        return null;
    }
}
