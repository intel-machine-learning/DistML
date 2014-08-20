package com.intel.word2vec.clusterps

import com.intel.word2vec.common.FloatOps
import scala.util.Random

/**
 * Created by He Yunlong on 7/12/14.
 */
class WordNodeData (
dim : Int
) {
  var index = 0

  var syn0 = new Array[Float](dim)
  var delta0 = new Array[Float](dim)
  var alpha0 = new Array[Float](dim)

  var syn1 = new Array[Float](dim)
  var delta1 = new Array[Float](dim)
  var alpha1 = new Array[Float](dim)

  def init(initialAlpha : Float) {

    var r = new Random()
    for (i <- 0 to Constants.MODEL_DIMENSION - 1) {
      val a = r.nextInt(100)
      syn0(i) = (a/100.0f - 0.5f) / Constants.MODEL_DIMENSION
      syn1(i) = 0.0f

      delta0(i) = 0.0f
      delta1(i) = 0.0f

      alpha0(i) = initialAlpha
      alpha1(i) = initialAlpha
    }
  }

  def clearDelta() {
    for (i <- 0 to Constants.MODEL_DIMENSION - 1) {
      delta0(i) = 0.0f
      delta1(i) = 0.0f
    }
  }


}

class W2VWorkerNodeData (
dim : Int
) extends WordNodeData(dim) {
  var next : W2VWorkerNodeData = null
  var deltaIndex = 0      // temp value for pushing delta
}
