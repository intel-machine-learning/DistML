package com.intel.distml.util.scala

import com.intel.distml.util.DataDesc

/**
 * Created by yunlong on 1/26/16.
 */
class FloatMatrixWithIntKey (
dim : Long,
cols: Int
) extends SparseMatrix[Int, Float](dim, cols, DataDesc.KEY_TYPE_INT, DataDesc.ELEMENT_TYPE_FLOAT) {

  override protected def isZero (value: Float): Boolean = {
    return Math.abs(value) < 10e-6
  }

  override protected def subtract(value: Float, delta : Float): Float = { value - delta }

  override protected def createValueArray (size: Int): Array[Float] = {
    return new Array[Float] (size)
  }

  override protected def toLong(k : Int) : Long = { k }

}