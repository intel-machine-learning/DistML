package com.intel.distml.model.word2vec

import scala.util.Random

/**
 * Created by He Yunlong on 7/12/14.
 */
/*
class WordNodeData (
dim : Int
) extends Serializable {
  var index = 0

  var syn0 = new Array[Float](dim)
  var delta0 = new Array[Float](dim)
  var alpha0 = new Array[Float](dim)

  var syn1 = new Array[Float](dim)
  var delta1 = new Array[Float](dim)
  var alpha1 = new Array[Float](dim)

  def init(initialAlpha : Float) {

    var r = new Random()
    for (i <- 0 to dim - 1) {
      val a = r.nextInt(100)
      syn0(i) = (a/100.0f - 0.5f) / dim
      syn1(i) = 0.0f

      delta0(i) = 0.0f
      delta1(i) = 0.0f

      alpha0(i) = initialAlpha
      alpha1(i) = initialAlpha
    }
  }

  def clear() {
    for (i <- 0 to dim- 1) {
      syn0(i) = 0.0f
      syn1(i) = 0.0f
    }
  }

  def clearDelta() {
    for (i <- 0 to dim - 1) {
      delta0(i) = 0.0f
      delta1(i) = 0.0f
    }
  }

  // ============ functions for training and updating =============
  def f(d : WordNodeData) : Float = {
    var f = 0.0f
    for (j <- 0 to dim-1) {
      f += syn0(j) * d.syn1(j)
    }
    f
  }

  def calculateDelta0(w : TempNodeData) {
    var tmp = 0.0f
    for (j <- 0 to dim-1) {
      tmp = w.data(j) * alpha0(j)
      syn0(j) += tmp
      delta0(j) += tmp
    }
  }

  def calculateDelta1(w : WordNodeData, g : Float) {
    var tmp = 0.0f
    for (c <- 0 to dim-1) {
      tmp = g * w.syn0(c) * alpha1(c)
      syn1(c) += tmp
      delta1(c) += tmp
    }
  }

  def calculateDelta1(d : TempNodeData, g : Float) {
    var tmp = 0.0f
    for (c <- 0 to dim-1) {
      tmp = g * d.data(c) * alpha1(c)
      syn1(c) += tmp
      delta1(c) += tmp
    }
  }

  def mergeDelta0(d : TempNodeData) {
    for (i <- 0 to dim -1) {
      val delta = d.data(i)
      syn0(i) += delta
      delta0(i) += delta * delta
      if (delta0(i) > 1.0) {
        alpha0(i) = Word2VecModel.initialAlpha / Math.sqrt(delta0(i)).toFloat
        if (alpha0(i) <= Word2VecModel.alphaThreshold)
          alpha0(i) = Word2VecModel.alphaThreshold
      }
    }
  }

  def mergeDelta1(d : TempNodeData) {
    for (i <- 0 to dim -1) {
      val delta = d.data(i)
      syn1(i) += delta
      delta1(i) += delta * delta
      if (delta1(i) > 1.0) {
        alpha1(i) = Word2VecModel.initialAlpha / Math.sqrt(delta1(i)).toFloat
        if (alpha1(i) <= Word2VecModel.alphaThreshold)
          alpha1(i) = Word2VecModel.alphaThreshold
      }
    }
  }
}
*/
class WordVectorWithAlpha (
                     dim : Int
                     ) extends Serializable {
  var index = 0

  var syn0 = new Array[Float](dim)
  var alpha0 = new Array[Float](dim)

  var syn1 = new Array[Float](dim)
  var alpha1 = new Array[Float](dim)

  def init(initialAlpha : Float) {

    var r = new Random()
    for (i <- 0 to dim - 1) {
      val a = r.nextInt(100)
      syn0(i) = (a/100.0f - 0.5f) / dim
      syn1(i) = 0.0f

      alpha0(i) = initialAlpha
      alpha1(i) = initialAlpha
    }
  }

  def clear() {
    for (i <- 0 to dim- 1) {
      syn0(i) = 0.0f
      syn1(i) = 0.0f
    }
  }

  // ============ functions for training and updating =============
  def f(d : WordVectorWithAlpha) : Float = {
    var f = 0.0f
    for (j <- 0 to dim-1) {
      f += syn0(j) * d.syn1(j)
    }
    f
  }

  def calculateDelta0(w : TempNodeData, u : WordVectorUpdate) {
    var tmp = 0.0f
    for (j <- 0 to dim-1) {
      tmp = w.data(j) * alpha0(j)
      syn0(j) += tmp
      u.delta0(j) += tmp
    }
  }

  def calculateDelta1(w : WordVectorWithAlpha, g : Float, u : WordVectorUpdate) {
    var tmp = 0.0f
    for (c <- 0 to dim-1) {
      tmp = g * w.syn0(c) * alpha1(c)
      syn1(c) += tmp
      u.delta1(c) += tmp
    }
  }

  def calculateDelta1(d : TempNodeData, g : Float, u : WordVectorUpdate) {
    var tmp = 0.0f
    for (c <- 0 to dim-1) {
      tmp = g * d.data(c) * alpha1(c)
      syn1(c) += tmp
      u.delta1(c) += tmp
    }
  }

  def mergeDelta0(d : TempNodeData, u : WordVectorUpdate) {
    for (i <- 0 to dim -1) {
      val delta = d.data(i)
      syn0(i) += delta
      u.delta0(i) += delta * delta
      if (u.delta0(i) > 1.0) {
        alpha0(i) = Word2VecModel.initialAlpha / Math.sqrt(u.delta0(i)).toFloat
        if (alpha0(i) <= Word2VecModel.alphaThreshold)
          alpha0(i) = Word2VecModel.alphaThreshold
      }
    }
  }

  def mergeDelta1(d : TempNodeData, u : WordVectorUpdate) {
    for (i <- 0 to dim -1) {
      val delta = d.data(i)
      syn1(i) += delta
      u.delta1(i) += delta * delta
      if (u.delta1(i) > 1.0) {
        alpha1(i) = Word2VecModel.initialAlpha / Math.sqrt(u.delta1(i)).toFloat
        if (alpha1(i) <= Word2VecModel.alphaThreshold)
          alpha1(i) = Word2VecModel.alphaThreshold
      }
    }
  }
}


class WordVector (
                     dim : Int
                     ) extends Serializable {
  var index = 0

  var syn0 = new Array[Float](dim)
  var syn1 = new Array[Float](dim)

  def init() {
    var r = new Random()
    for (i <- 0 to dim - 1) {
      val a = r.nextInt(100)
      syn0(i) = (a/100.0f - 0.5f) / dim
      syn1(i) = 0.0f
    }
  }

  def clear() {
    for (i <- 0 to dim- 1) {
      syn0(i) = 0.0f
      syn1(i) = 0.0f
    }
  }
}

class WordVectorUpdate (
                     dim : Int
                     ) extends Serializable {
  var index = 0

  var delta0 = new Array[Float](dim)
  var delta1 = new Array[Float](dim)

  def init(): Unit = {
    clear();
  }

  def clear() {
    for (i <- 0 to dim - 1) {
      delta0(i) = 0.0f
      delta1(i) = 0.0f
    }
  }
}



class TempNodeData (
dim : Int
) {
  var data = new Array[Float](dim)

  def clear() {
    for (i <- 0 to dim - 1) {
      data(i) = 0.0f
    }
  }

  def addFromSyn0(d : WordVectorWithAlpha) {
    for (c <- 0 to dim-1) {
      data(c) += d.syn0(c)
    }
  }

  def accumSyn1(d : WordVectorWithAlpha, g : Float) {
    for (c <- 0 to dim-1) {
      data(c) += g * d.syn1(c)
    }
  }

  def dotProductSyn1(d : WordVectorWithAlpha) : Float = {
    var f = 0.0f
    for (j <- 0 to dim-1) {
      f += data(j) * d.syn1(j)
    }
    f
  }

//  def deserialize(dataBytes : Array[Byte]) {
//    for (i <- 0 to dim -1) {
//      data(i) = FloatOps.getFloatFromBytes(dataBytes, i * 4)
//    }
//  }
}
