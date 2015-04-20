package com.intel.distml.model.word2vec

/**
 * Created by He Yunlong on 7/11/14.
 */
class HuffmanNode extends Serializable {
  val MAX_CODE_LENGTH = 40

  var name : String = ""

  var index = 0
  var binary = 0
  var frequency = 0L
  var parentIndex = 0

  var codeLen = 0
  var code = new Array[Int](MAX_CODE_LENGTH)
  var point = new Array[Int](MAX_CODE_LENGTH)

  override def clone() : HuffmanNode = {
    val w = new HuffmanNode()
    w.index = index
    w.codeLen = codeLen
    for (i <- 0 to MAX_CODE_LENGTH - 1) {
      w.code(i) = code(i)
      w.point(i) = point(i)
    }

    w
  }

}
