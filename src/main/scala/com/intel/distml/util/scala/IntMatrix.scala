package com.intel.distml.util.scala

import com.intel.distml.util.DataDesc

/**
 * Created by yunlong on 1/26/16.
 */
class IntMatrix (
dim : Long,
cols: Int
) extends SparseMatrix[Long, Int](dim, cols, DataDesc.KEY_TYPE_LONG, DataDesc.ELEMENT_TYPE_INT) {

  protected def isZero (value: Int): Boolean = {
    return value == 0
  }

  override protected def subtract(value: Int, delta : Int): Int = { value - delta }

  protected def createValueArray (size: Int): Array[Int] = {
    return new Array[Int] (size)
  }

  protected def toLong(k : Long) : Long = { k }

}