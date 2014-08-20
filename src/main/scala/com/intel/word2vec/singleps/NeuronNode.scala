package com.intel.word2vec.singleps

import java.nio.ByteBuffer
import scala.collection.mutable
import scala.util.Random
import com.intel.word2vec.common.FloatOps

/**
 * Created by He Yunlong on 4/23/14.
 */
class NeuronNode extends Serializable {

  val MAX_CODE_LENGTH = 40

  var name : String = ""
  var loaded = false

  var index = 0
  var binary = 0
  var frequency = 0L
  var parentIndex = 0

  var codeLen = 0
  var code = new Array[Int](MAX_CODE_LENGTH)
  var point = new Array[Int](MAX_CODE_LENGTH)

  var syn0 : Array[Float] = null
  var syn1 : Array[Float] = null
  var delta0 : Array[Float] = null
  var delta1 : Array[Float] = null
  var alpha0 : Array[Float] = null
  var alpha1 : Array[Float] = null

  var newSyn0TrainCount : Int = 0
  var newSyn1TrainCount : Int = 0

  def createVectors() {

      syn0 = new Array[Float](200)
      syn1 = new Array[Float](200)

      delta0 = new Array[Float](200)
      delta1 = new Array[Float](200)
      alpha0 = new Array[Float](200)
      alpha1 = new Array[Float](200)
  }

  def initVectors(alpha : Float) {
      var r = new Random
      for (i <- 0 to 199) {
        val a = r.nextInt(100)
        syn0(i) = (a/100.0f - 0.5f) / 200.0f
        syn1(i) = 0.0f

          delta0(i) = 0.0f
          delta1(i) = 0.0f
          alpha0(i) = alpha
          alpha1(i) = alpha
        }
  }

}
