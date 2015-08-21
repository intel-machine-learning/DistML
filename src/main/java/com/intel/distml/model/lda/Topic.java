package com.intel.distml.model.lda;

import com.intel.distml.util.GeneralArray;
import com.intel.distml.util.KeyCollection;
import com.intel.distml.util.KeyRange;

/**
 * Created by ruixiang on 7/8/15.
 */
public class Topic extends GeneralArray<Integer> {
    public Topic(Integer[] _topic,KeyRange keys){
        super(_topic,keys);
    }
    public Topic(){

    }
    @Override
    public Object clone(){
        if(this.rowKeys instanceof KeyCollection.EMPTY_KEYS)return null;
        Topic t=new Topic(this.values.clone(),(KeyRange)this.rowKeys);
        return t;
    }
    public void mergeUpdate(int serverIndex,Topic t){
        long first=((KeyRange)rowKeys).firstKey;
        long last=((KeyRange)rowKeys).lastKey;
        long keySize=last-first+1;
        System.out.println("this range:" + first + "-" + last);
        System.out.println("push range:"+((KeyRange)t.rowKeys).firstKey+"-"+((KeyRange)t.rowKeys).lastKey);

        for(int i=(int)first;i<=last;i++){
            this.values[i-(int)keySize*serverIndex]=this.values[i-(int)keySize*serverIndex]+t.values[i-(int)keySize*serverIndex];
        }
    }
}