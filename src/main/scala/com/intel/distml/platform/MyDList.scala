package com.intel.distml.platform

import com.intel.distml.util.DList

/**
 * Created by yunlong on 7/17/15.
 */
class MyDList[T] extends DList[T]{

  var data = new scala.collection.mutable.ListBuffer[T]

  def add(t : T): Unit = {
    data += t
  }

  def iterator() : Iterator[T] = {
    data.iterator
  }

}
