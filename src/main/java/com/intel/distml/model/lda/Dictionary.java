package com.intel.distml.model.lda;

/**
 * Created by ruixiang on 5/18/15.
 */

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Dictionary implements Serializable {
    public Map<String, Integer> word2id;
    public Map<Integer, String> id2word;

    //--------------------------------------------------
    // constructors
    //--------------------------------------------------

    public Dictionary() {
        word2id = new HashMap<String, Integer>();
        id2word = new HashMap<Integer, String>();
    }

    //---------------------------------------------------
    // get/set methods
    //---------------------------------------------------
    public int getSize() {
        return word2id.size();
    }

    public String getWord(int id) {
        return id2word.get(id);
    }

    public Integer getID(String word) {
        return word2id.get(word);
    }

    //----------------------------------------------------
    // checking methods
    //----------------------------------------------------

    /**
     * check if this dictionary contains a specified word
     */
    public boolean contains(String word) {
        return word2id.containsKey(word);
    }

    public boolean contains(int id) {
        return id2word.containsKey(id);
    }
    //---------------------------------------------------
    // manupulating methods
    //---------------------------------------------------

    /**
     * add a word into this dictionary
     * return the corresponding id
     */
    public int addWord(String word) {
        if (!contains(word)) {
            int id = word2id.size();

            word2id.put(word, id);
            id2word.put(id, word);

            return id;
        } else return getID(word);
    }

}