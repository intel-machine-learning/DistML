package com.intel.distml.model.word2vec

/**
 * Created by He Yunlong on 9/15/14.
 */
class WordNode extends Serializable {
  var name : String = null
  var index = 0
  var codeLen = 0
  var code: Array[Int] = null
  var point: Array[Int] = null

  def initFrom( w : HuffmanNode) {
    name = w.name
    index = w.index
    codeLen = w.codeLen
    if (codeLen > 0) {
      code = new Array[Int](codeLen)
      point = new Array[Int](codeLen)
      for (i <- 0 to codeLen - 1) {
        code(i) = w.code(i)
      }
      for (i <- 0 to codeLen - 1) {
        point(i) = w.point(i)
      }
    }
  }

  override def clone() : WordNode = {
    val w = new WordNode()
    w.name = name
    w.index = index
    w.codeLen = codeLen
    if (codeLen > 0) {
      w.code = new Array[Int](codeLen)
      w.point = new Array[Int](codeLen)
      for (i <- 0 to codeLen - 1) {
        w.code(i) = code(i)
      }
      for (i <- 0 to codeLen - 1) {
        w.point(i) = point(i)
      }
    }
    w
  }
}
