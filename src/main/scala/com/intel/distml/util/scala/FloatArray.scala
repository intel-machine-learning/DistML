package com.intel.distml.util.scala

import com.intel.distml.util.DataDesc

/**
 * Created by yunlong on 1/3/16.
 */
class FloatArray (
dim : Long) extends SparseArray[Long, Float](dim, DataDesc.KEY_TYPE_LONG, DataDesc.ELEMENT_TYPE_FLOAT) {

  protected def toLong(k : Long) : Long  = k

}
