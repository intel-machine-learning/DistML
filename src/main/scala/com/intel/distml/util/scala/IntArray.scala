package com.intel.distml.util.scala

import com.intel.distml.util.DataDesc

/**
 * Created by yunlong on 1/3/16.
 */
class IntArray (
dim : Long) extends SparseArray[Long, Int](dim, DataDesc.KEY_TYPE_LONG, DataDesc.ELEMENT_TYPE_INT) {

  protected def toLong(k : Long) : Long  = k

}
