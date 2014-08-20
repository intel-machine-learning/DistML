package com.intel.word2vec.clusterps


import scala.collection.mutable._

/**
 * Created by He Yunlong on 7/11/14.
 * for 4*10^7 words, it uses at least 16 * 10^7 = 160M bytes
 */

class WordTree(
val vocabSize : Int,
val words : Array[WordNode],
dim : Int
) extends Serializable{


  def getWord(index : Int) : WordNode = {
    return words(index)
  }

  def nodeCount() : Int = {
    return words.length
  }

  def bufferingDone() {

  }

  override def clone() :WordTree = {
    var ws = new Array[WordNode](words.length)
    for (i <- 0 to words.length - 1) {
      ws(i) = words(i).clone()
    }

    new WordTree(vocabSize, ws, dim)
  }
}
