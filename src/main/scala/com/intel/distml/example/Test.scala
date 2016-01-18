package com.intel.distml.example

import com.intel.distml.api.Model
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by yunlong on 1/15/16.
 */
object Test {

  def f(index: Int) : Int = {

    println("starting server task")

    Thread.sleep(2000)

    println("stopping server task")
    1
  }

  def main(args : Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparseTest").set("spark.default.parallelism", "3")
    val sc = new SparkContext(conf)

    var psCount = 3
    if (args.length > 0) {
      psCount = Integer.parseInt(args(0))
    }

    var da = new Array[Int](psCount)
    for (i <- 0 to psCount - 1)
      da(i) = i

    val data = sc.parallelize(da, psCount)
    println("prepare to start parameter servers: " + data.partitions.length)
    for (i <- 0 to data.partitions.length-1) {
      val p = data.partitions(i)
      println("partition: " + p)
    }
    val dummy = data.map(f).collect()
    println("parameter servers finish their work.")

    sc.stop()
  }


}
