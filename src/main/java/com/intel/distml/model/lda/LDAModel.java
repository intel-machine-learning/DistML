package com.intel.distml.model.lda;

import com.intel.distml.api.DMatrix;
import com.intel.distml.api.Model;
import com.intel.distml.api.databus.DataBus;
import com.intel.distml.util.*;
import scala.collection.mutable.ListBuffer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by ruixiang on 5/17/15.
 */
public class LDAModel extends Model {
    public static final String MATRIX_PARAM_WORDTOPIC = "PARAM_WORDTOPIC";
    public static final String MATRIX_PARAM_TOPIC = "PARAM_TOPIC";

    private float alpha;
    private float beta;
    private int K;
    private int vocabularySize;

    private float[] p;//temp variables for sampling

    private volatile double[][] phi;

    public LDAModel(float _alpha, float _beta, int _K, int _v) {

        dataSetImmutable = false;
        this.autoFetchParams = false;
        this.autoPushUpdates = false;

        this.alpha = _alpha;
        this.beta = _beta;
        this.K = _K;
        this.p = new float[_K];
        this.vocabularySize = _v;

        registerMatrix(LDAModel.MATRIX_PARAM_WORDTOPIC, new ParamWordTopic(vocabularySize, K));
        registerMatrix(LDAModel.MATRIX_PARAM_TOPIC, new ParamTopic(K));

    }

    @Override
    public Matrix transformSamples(List<Object> samples) {
        System.out.println("LDA transform sample to Matrix: ");
        return new Blob<List>(samples);
    }

    @Override
    public void compute(Matrix samples, int workerIndex, DataBus dataBus, final int iterationIndex, DList result) {

        List<LDADataMatrix> sentences = ((Blob<List>) samples).element();
        while (sentences.size() > 0) {
            LDADataMatrix s = sentences.remove(0);
            computeDoc(s, workerIndex, dataBus, iterationIndex, result);
        }

    }

    public void computeDoc(LDADataMatrix ldaData, int workerIndex, DataBus dataBus, final int iterationIndex, DList result) {

        System.out.println("prefetch word-topic parameter");
        KeyList keys = new KeyList();
        for (int i = 0; i < ldaData.words.length; i++)
            keys.addKey(ldaData.words[i]);

        HashMapMatrix wordTopics = (HashMapMatrix) dataBus.fetchFromServer(LDAModel.MATRIX_PARAM_WORDTOPIC, keys);
        HashMapMatrix wordTopicsUpdate = null;
        try {
            wordTopicsUpdate = (HashMapMatrix) wordTopics.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }

        System.out.println("prefetch topic parameter");
        Topic topics = (Topic) dataBus.fetchFromServer(LDAModel.MATRIX_PARAM_TOPIC, KeyCollection.ALL);
        Topic topicsUpdate = (Topic) topics.clone();

        if (iterationIndex == 0) {
            result.add(initParam(ldaData, wordTopics, topics));
        } else {
            result.add(sampling(ldaData, wordTopics, topics));
        }
        //compute and push word-topic update

        Iterator itr = wordTopicsUpdate.getRowKeys().iterator();
        while (itr.hasNext()) {
            Long key = (Long) itr.next();
            Integer[] newValue = (Integer[]) wordTopics.get(key);
            Integer[] oldValue = (Integer[]) wordTopicsUpdate.get(key);
            for (int i = 0; i < newValue.length; i++)
                oldValue[i] = newValue[i] - oldValue[i];
        }
        dataBus.pushUpdate(LDAModel.MATRIX_PARAM_WORDTOPIC, wordTopicsUpdate);

        //compute and push topic update

        for (int i = 0; i < topicsUpdate.values.length; i++)
            topicsUpdate.values[i] = topics.values[i] - topicsUpdate.values[i];
        dataBus.pushUpdate(LDAModel.MATRIX_PARAM_TOPIC, topicsUpdate);

    }

    //Help functions
    LDADataMatrix sampling(LDADataMatrix ldaData, HashMapMatrix wordTopics, Topic topics) {

        Integer[] numTopic = (Integer[]) topics.values;
        int[] numDocTopic = ldaData.nDocTopic;
        for (int i = 0; i < ldaData.words.length; i++) {

            int topic = ldaData.topics[i];
            int wordID = ldaData.words[i];

            Integer[] thisWordTopics = (Integer[]) wordTopics.get(wordID);

            numTopic[topic]--;
            thisWordTopics[topic]--;
            numDocTopic[topic]--;

            float Vbeta = vocabularySize * this.beta;
            float Kalpha = this.K * this.alpha;

            for (int k = 0; k < K; k++) {
                this.p[k] = (thisWordTopics[k] + beta) / (numTopic[k] + Vbeta) *
                        (numDocTopic[k] + alpha) / (ldaData.words.length - 1 + Kalpha);
            }

            for (int k = 1; k < K; k++) {
                this.p[k] += this.p[k - 1];
            }

            double u = Math.random() * this.p[K - 1];

            for (topic = 0; topic < K; topic++) {
                if (p[topic] >= u) break;
            }
            if (topic >= K) topic = K - 1;

            numTopic[topic]++;
            thisWordTopics[topic]++;
            numDocTopic[topic]++;

            ldaData.topics[i] = topic;
        }

        return ldaData;
    }

    LDADataMatrix initParam(LDADataMatrix ldaData, HashMapMatrix wordTopics, Topic topics) {
        System.out.println("init LDA Param");

        Integer[] numTopic = (Integer[]) topics.values;
        for (int i = 0; i < ldaData.words.length; i++) {
            Integer[] thisWordTopics = (Integer[]) wordTopics.get(ldaData.words[i]);
            int thisTopic = ldaData.topics[i];
            thisWordTopics[thisTopic]++;
            numTopic[ldaData.topics[i]]++;
        }
        return ldaData;
    }

    public void getPhi() {

        Integer[][] wt = ((WordTopic) getCache(LDAModel.MATRIX_PARAM_WORDTOPIC)).values;
        Integer[] t = ((Topic) getCache(LDAModel.MATRIX_PARAM_TOPIC)).values;

        phi = new double[t.length][wt.length];
        for (int k = 0; k < K; k++) {
            for (int w = 0; w < wt.length; w++) {
                phi[k][w] = (wt[w][k] + beta) / (t[k] + wt.length * beta);
            }
        }
    }

    public double[] inference(int[] doc, int iterations) {

        if (phi == null) {
            getPhi();
        }

        int K = phi.length;
        int V = phi[0].length;

        int[][] nw = new int[V][K];
        int[] nd = new int[K];
        int[] nwsum = new int[K];
        int ndsum = 0;

        // The z_i are are initialised to values in [1,K] to determine the
        // initial state of the Markov chain.

        int N = doc.length;
        int[] z = new int[N];   // z_i := 1到K之间的值，表示马氏链的初始状态
        for (int n = 0; n < N; n++) {
            int topic = (int) (Math.random() * K);
            z[n] = topic;
            // number of instances of word i assigned to topic j
            nw[doc[n]][topic]++;
            // number of words in document i assigned to topic j.
            nd[topic]++;
            // total number of words assigned to topic j.
            nwsum[topic]++;
        }
        // total number of words in document i
        ndsum = N;
        for (int i = 0; i < iterations; i++) {
            for (int n = 0; n < z.length; n++) {

                // (z_i = z[m][n])
                // sample from p(z_i|z_-i, w)
                // remove z_i from the count variables  先将这个词从计数器中抹掉
                int topic = z[n];
                nw[doc[n]][topic]--;
                nd[topic]--;
                nwsum[topic]--;
                ndsum--;

                // do multinomial sampling via cumulative method: 通过多项式方法采样多项式分布
                double[] p = new double[K];
                for (int k = 0; k < K; k++) {
                    p[k] = (nw[doc[n]][k] + beta) / (nwsum[k] + V * beta)
                            * (nd[k] + alpha) / (ndsum + K * alpha);
                }
                // cumulate multinomial parameters  累加多项式分布的参数
                for (int k = 1; k < p.length; k++) {
                    p[k] += p[k - 1];
                }
                // scaled sample because of unnormalised p[] 正则化
                double u = Math.random() * p[K - 1];
                for (topic = 0; topic < p.length; topic++) {
                    if (u < p[topic])
                        break;
                }

                // add newly estimated z_i to count variables   将重新估计的该词语加入计数器
                nw[doc[n]][topic]++;
                nd[topic]++;
                nwsum[topic]++;
                ndsum++;
                z[n] = topic;
            }
        }

        double[] theta = new double[K];

        for (int k = 0; k < K; k++) {
            theta[k] = (nd[k] + alpha) / (ndsum + K * alpha);
        }
        return theta;
    }

}
