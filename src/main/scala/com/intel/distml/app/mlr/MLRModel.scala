package com.intel.distml.app.mlr

import com.intel.distml.model.sparselr.{SparseLRModel, LRSample}
import com.intel.distml.util.Matrix

/**
 * Created by yunlong on 2/28/15.
 */
class MLRModel extends SparseLRModel(MLR.MLR_DIM) {


  /**
   *
   * 7 1:0.830916 2:-0.845769 3:-0.680908 4:0.430816 5:0.455918 6:-0.298958 7:0.554725 8:-0.0667678 9:-0.379671 10:0.613741 11:1 43:1
     2 1:-0.612044 2:-0.988741 3:-0.146862 4:-0.0678899 5:0.0614065 6:-0.499054 7:0.405304 8:-0.673748 9:-0.614842 10:0.430233 11:1 43:1
     2 1:0.24516 2:1.05755 3:1.98932 4:-0.279605 5:0.953345 6:0.112137 7:-2.99402 8:0.489631 9:2.36399 10:0.541244 13:1 47:1
     7 1:1.38453 2:-0.604504 3:-0.0133511 4:-0.561891 5:-0.538937 6:0.256437 7:1.04034 8:-0.370258 9:-1.03292 10:-0.1943 13:1 52:1
     2 1:1.10236 2:0.735859 3:0.654206 4:1.95046 5:2.23979 6:1.02219 7:-1.12626 8:1.50126 9:1.60622 10:-0.921537 13:1 48:1
     1 1:0.091578 2:1.07542 3:-1.34846 4:-0.867702 5:-0.659005 6:2.14388 7:-0.155024 8:0.843703 9:0.743925 10:0.0662367 11:1 36:1
     1 1:-0.0977213 2:0.575016 3:-0.814419 4:-0.820654 5:-0.607547 6:-0.139266 7:-0.0429585 8:1.24836 9:0.822315 10:-0.13162 13:1 37:1
   * @param sampleList
   * @return
   */
  override def transformSamples(sampleList: java.util.List[AnyRef]): Matrix = {
    val strObj = sampleList.get(0);
    val str: String = strObj.asInstanceOf[String]
    System.out.println("sample string: [" + str + "]")
    val strs: Array[String] = str.split(" ")
    val sample: LRSample = new LRSample(MLR.MLR_DIM)
    sample.label = strs(0).toDouble

    for (i <- 0 to strs.length-1) {
      val tmp: Array[String] = strs(i).split(":")
      val key: Long = tmp(0).toInt
      val value: Double = tmp(1).toDouble
      sample.data().put(key, value)
    }

    return sample
  }
}
