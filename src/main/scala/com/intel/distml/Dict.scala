package com.intel.distml

import java.io.Serializable

import scala.collection.immutable

/**
 * Created by yunlong on 12/23/15.
 */
class Dict extends Serializable {

  var  word2id = new immutable.HashMap[String, Int]
  var  id2word = new immutable.HashMap[Int, String]

  def getSize: Int = {
    return word2id.size
  }

  def getWord(id: Int): String = {
    return id2word.get(id).get
  }

  def getID(word: String): Integer = {
    return word2id.get(word).get
  }

  /**
   * check if this dictionary contains a specified word
   */
  def contains(word: String): Boolean = {
    return word2id.contains(word)
  }

  def contains(id: Int): Boolean = {
    return id2word.contains(id)
  }

  def put(word : String, id : Int): Unit = {
    word2id += (word -> id)
    id2word += (id -> word)
  }

  /**
   * add a word into this dictionary
   * return the corresponding id
   */
  def addWord(word: String): Int = {
    if (!contains(word)) {
      val id: Int = word2id.size
      word2id += (word -> id)
      id2word += (id -> word)
      return id
    }
    else return getID(word)
  }
}