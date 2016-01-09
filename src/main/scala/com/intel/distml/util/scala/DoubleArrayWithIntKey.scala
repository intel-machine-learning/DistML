package com.intel.distml.util.scala

import com.intel.distml.util.DataDesc

/**
  * Created by yunlong on 1/3/16.
  */
class DoubleArrayWithIntKey(
 dim : Long) extends SparseArray[Int, Double](dim, DataDesc.KEY_TYPE_INT, DataDesc.ELEMENT_TYPE_DOUBLE) {

 protected def toLong(k : Int) : Long  = k

  }
