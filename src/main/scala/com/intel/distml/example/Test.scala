package com.intel.distml.example

import java.util
import scala.collection.JavaConversions._

/**
 * Created by yunlong on 12/28/15.
 */
object Test {

  def main(args: Array[String]): Unit = {

    val dt = new util.HashMap[java.lang.Long, Integer]
    val wt = new util.HashMap[java.lang.Long, Array[Integer]]
    println("before computing, dt=" + dt)
    for (i <- 0 to 9) {
      println("dt(" + i + ")=" + dt.get(i.toLong))
    }

    for (i <- 0 to 9) {
      dt.put(i.toLong, new Integer(0))
    }
    println("after initialized, dt=" + dt)
    for (i <- 0 to 9) {
      println("dt(" + i + ")=" + dt.get(i))
    }
  }
}
