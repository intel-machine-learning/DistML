package com.intel.distml.util.scala

import com.intel.distml.util.DataDesc

/**
  * Created by jimmy on 16-4-7.
  */
class DoubleMatrixWithIntKey
(
  dim: Long,
  cols: Int
) extends SparseMatrix [Int,Double](dim, cols, DataDesc.KEY_TYPE_INT, DataDesc.ELEMENT_TYPE_DOUBLE){

  override protected def isZero (value: Double): Boolean = {
    return Math.abs(value) < 10e-6
  }

  override protected def subtract(value: Double, delta : Double): Double = { value - delta }

  override protected def createValueArray (size: Int): Array[Double] = {
    return new Array[Double] (size)
  }

  override protected def toLong(k : Int) : Long = { k }
}
