package com.intel.word2vec.clusterps

/**
 * Created by He Yunlong on 7/11/14.
 */
class WordNode extends Serializable {
  val MAX_CODE_LENGTH = 40

  var name : String = ""

  var index = 0
  var binary = 0
  var frequency = 0L
  var parentIndex = 0

  var codeLen = 0
  var code = new Array[Int](MAX_CODE_LENGTH)
  var point = new Array[Int](MAX_CODE_LENGTH)

  var data : WordNodeData = null

  override def clone() : WordNode = {
    val w = new WordNode()
    w.index = index
    w.codeLen = codeLen
    for (i <- 0 to MAX_CODE_LENGTH - 1) {
      w.code(i) = code(i)
      w.point(i) = point(i)
    }

    w
  }
}
