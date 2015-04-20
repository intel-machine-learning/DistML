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

  def clear() {
    for (i <- 0 to Constants.MODEL_DIMENSION - 1) {
      syn0(i) = 0.0f
      syn1(i) = 0.0f
    }
  }

  def clearDelta() {
    for (i <- 0 to Constants.MODEL_DIMENSION - 1) {
      delta0(i) = 0.0f
      delta1(i) = 0.0f
    }
  }

  // ============ functions for training and updating =============
  def f(d : WordNodeData) : Float = {
    var f = 0.0f
    for (j <- 0 to Constants.MODEL_DIMENSION-1) {
      f += syn0(j) * d.syn1(j)
    }
    f
  }

  def calculateDelta0(neu1e : TempNodeData) {
    var tmp = 0.0f
    for (j <- 0 to Constants.MODEL_DIMENSION-1) {
      tmp = neu1e.data(j) * alpha0(j)
      syn0(j) += tmp
      delta0(j) += tmp
    }
  }

  def calculateDelta1(lastWord : WordNodeData, g : Float) {
    var tmp = 0.0f
    for (c <- 0 to Constants.MODEL_DIMENSION-1) {
      tmp = g * lastWord.syn0(c) * alpha1(c)
      syn1(c) += tmp
      delta1(c) += tmp
    }
  }


  def mergeDelta0(d : TempNodeData) {
    for (i <- 0 to Constants.MODEL_DIMENSION -1) {
      val delta = d.data(i)
      syn0(i) += delta
      delta0(i) += delta * delta
      if (delta0(i) > 1.0) {
        alpha0(i) = Constants.initialAlpha / Math.sqrt(delta0(i)).toFloat
        if (alpha0(i) <= Constants.alphaThreshold)
          alpha0(i) = Constants.alphaThreshold
      }
    }
  }

  def mergeDelta1(d : TempNodeData) {
    for (i <- 0 to Constants.MODEL_DIMENSION -1) {
      val delta = d.data(i)
      syn1(i) += delta
      delta1(i) += delta * delta
      if (delta1(i) > 1.0) {
        alpha1(i) = Constants.initialAlpha / Math.sqrt(delta1(i)).toFloat
        if (alpha1(i) <= Constants.alphaThreshold)
          alpha1(i) = Constants.alphaThreshold
      }
    }
  }
}

class TempNodeData (
) {
  var data = new Array[Float](Constants.MODEL_DIMENSION)

  def clear() {
    for (i <- 0 to Constants.MODEL_DIMENSION - 1) {
      data(i) = 0.0f
    }
  }

  def accum(d : W2VWorkerNodeData, g : Float) {
    for (c <- 0 to Constants.MODEL_DIMENSION-1) {
      data(c) += g * d.syn1(c)
    }
  }

  def deserialize(dataBytes : Array[Byte]) {
    for (i <- 0 to Constants.MODEL_DIMENSION -1) {
      data(i) = FloatOps.getFloatFromBytes(dataBytes, i * 4)
    }
  }
}

class W2VWorkerNodeData (
dim : Int
) extends WordNodeData(dim) {
  var next : W2VWorkerNodeData = null
  var deltaIndex = 0      // temp value for pushing delta
}
