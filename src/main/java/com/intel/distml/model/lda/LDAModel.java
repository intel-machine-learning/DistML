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
    public Dictionary dict;


    private float[] p;//temp variables for sampling

    public LDAModel(float _alpha,float _beta,int _K,Dictionary _dict){

        dataSetImmutable = false;
        this.autoFetchParams=false;
        this.autoPushUpdates=false;

        this.alpha=_alpha;
        this.beta=_beta;
        this.K=_K;
        this.dict=_dict;
        this.p=new float[_K];

        registerMatrix(LDAModel.MATRIX_PARAM_WORDTOPIC,new ParamWordTopic(dict.getSize(),K));
        registerMatrix(LDAModel.MATRIX_PARAM_TOPIC,new ParamTopic(K));

    }

    @Override
    public Matrix transformSamples(List<Object> samples) {
        System.out.println("LDA transform sample to Matrix");
        Object line=samples.get(0);
        LDADataMatrix data=null;
//        if(line instanceof String){
//            //the first iterator,not executed
//            String[] words=((String) line).split(" ");
//            int[] wordsID=new int[words.length];
//            int[] topics=new int[words.length];
//            int[] numTopic=new int[K];
//
//            for(int i=0;i<words.length;i++){
//                int id=dict.getID(words[i]);
//                wordsID[i]=id;
//
//                int topic = (int)Math.floor(Math.random() * K);
//                topics[i]=topic;
//            }
//
//            for(int i=0;i<topics.length;i++){
//                numTopic[topics[i]]++;
//            }
//            data=new LDADataMatrix(topics,wordsID,numTopic);
//        }else{
        data=(LDADataMatrix)samples.get(0);

        return data;
    }

    @Override
    public void compute(Matrix sample, int workerIndex, DataBus dataBus,final int iterationIndex, DList result) {
        System.out.println("=======compute in worker index: " + workerIndex + " ========");
        LDADataMatrix ldaData=(LDADataMatrix)sample;

        System.out.println("prefetch word-topic parameter");
        KeyList keys=new KeyList();
        for(int i=0;i<ldaData.words.length;i++)keys.addKey(ldaData.words[i]);

        HashMapMatrix wordTopics = (HashMapMatrix) dataBus.fetchFromServer(LDAModel.MATRIX_PARAM_WORDTOPIC,keys);
        HashMapMatrix wordTopicsUpdate = null;
        try {
            wordTopicsUpdate = (HashMapMatrix)wordTopics.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }

        System.out.println("prefetch topic parameter");
        Topic topics= (Topic)dataBus.fetchFromServer(LDAModel.MATRIX_PARAM_TOPIC,KeyCollection.ALL);
        Topic topicsUpdate=(Topic)topics.clone();

        if(iterationIndex==0){
            result.add(initParam(ldaData,wordTopics,topics));
        }
        else{
            result.add(sampling(ldaData, wordTopics,topics));
        }
        //compute and push word-topic update

        Iterator itr = wordTopicsUpdate.getRowKeys().iterator();
        while (itr.hasNext()){
            Long key=(Long)itr.next();
            Integer[] newValue=(Integer[])wordTopics.get(key);
            Integer[] oldValue=(Integer[])wordTopicsUpdate.get(key);
            for(int i=0;i<newValue.length;i++)
                oldValue[i]=newValue[i]-oldValue[i];
        }
        dataBus.pushUpdate(LDAModel.MATRIX_PARAM_WORDTOPIC,wordTopicsUpdate);

        //compute and push topic update

        for(int i=0;i<topicsUpdate.values.length;i++)
            topicsUpdate.values[i]=topics.values[i]-topicsUpdate.values[i];
        dataBus.pushUpdate(LDAModel.MATRIX_PARAM_TOPIC,topicsUpdate);

    }
    //Help functions
    LDADataMatrix sampling(LDADataMatrix ldaData,HashMapMatrix wordTopics,Topic topics){
//        System.out.print("before gibbs sampling,the topics are ");
//        for(int i=0;i<ldaData.topics.length;i++)System.out.print(ldaData.topics[i]+",");
//        System.out.println();
//
//        System.out.print("before gibbs sampling topic array:");
//        for(int i=0;i<K;i++)System.out.print(topics.element(i)+",");
//        System.out.println();

        long start=System.currentTimeMillis();

        Integer[] numTopic=(Integer[])topics.values;
        int[] numDocTopic=ldaData.nDocTopic;
        for(int i=0;i<ldaData.words.length;i++){

            int topic=ldaData.topics[i];
            int wordID=ldaData.words[i];

            Integer[] thisWordTopics=(Integer[])wordTopics.get(wordID);

            numTopic[topic]--;
            thisWordTopics[topic]--;
            numDocTopic[topic]--;

            float Vbeta = dict.getSize() * this.beta;
//            System.out.println("Vbeta:" + Vbeta + "beta: " + this.beta +" dict size:" +dict.getSize());
            float Kalpha = this.K * this.alpha;

            for(int k=0;k<K;k++){
                this.p[k] = (thisWordTopics[k] + beta) / (numTopic[k] + Vbeta) *
                        (numDocTopic[k] + alpha) / (ldaData.words.length - 1 + Kalpha);
//               if(wordID==0)System.out.println("======" + (thisWordTopics[k] + beta) + "/" + numTopic[k] + "+" + Vbeta +
//                       "*" + (numDocTopic[k] + alpha) + "/" + (ldaData.words.length - 1 + Kalpha) + "===the P is: "+ this.p[k] +"========");
            }

            for(int k=1;k<K;k++){
                this.p[k]+=this.p[k-1];
            }

            double u=Math.random()*this.p[K-1];
//            double u=0.4*this.p[K-1];

            for(topic=0;topic<K;topic++){
                if(p[topic]>=u)break;
            }

            if(topic==K)System.out.println("max p: " +this.p[K-1] + " u: " + u);
            numTopic[topic]++;
            thisWordTopics[topic]++;
            numDocTopic[topic]++;

            ldaData.topics[i]=topic;
        }

//        System.out.print("after gibbs sampling topic array:");
//        for(int i=0;i<K;i++)System.out.print(topics.element(i)+",");
//        System.out.println();
//
//        System.out.print("after gibbs sampling,the topics are ");
//        for(int i=0;i<ldaData.topics.length;i++)System.out.print(ldaData.topics[i]+",");
//        System.out.println();

        System.out.println("LDA compute with time :" + (System.currentTimeMillis() - start));
        return ldaData;
    }

    LDADataMatrix initParam(LDADataMatrix ldaData,HashMapMatrix wordTopics,Topic topics){
        System.out.println("init LDA Param");

        Integer[] numTopic=(Integer[])topics.values;
        for(int i=0;i<ldaData.words.length;i++){
            Integer[] thisWordTopics=(Integer[])wordTopics.get(ldaData.words[i]);
            int thisTopic=ldaData.topics[i];
            thisWordTopics[thisTopic]++;
            numTopic[ldaData.topics[i]]++;
        }
        return ldaData;
    }
}
