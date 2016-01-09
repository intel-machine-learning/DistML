package com.intel.distml.util.scala

import com.intel.distml.util.DataDesc

/**
 * Created by yunlong on 1/3/16.
 */
class DoubleArray (
dim : Long) extends SparseArray[Long, Double](dim, DataDesc.KEY_TYPE_LONG, DataDesc.ELEMENT_TYPE_DOUBLE) {

  protected def toLong(k : Long) : Long  = k

}
