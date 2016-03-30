package com.intel.distml.regression

import java.util

import com.intel.distml.api.{Model, Session}
import com.intel.distml.platform.DistML
import com.intel.distml.util.{KeyCollection, DoubleMatrix, KeyList, DataStore}
import com.intel.distml.util.scala.DoubleArrayWithIntKey
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * Created by yunlong on 16-3-30.
 */

class MLRModel(
val inputDim : Long,
val outputDim : Int
) extends Model {

  registerMatrix("weights", new DoubleMatrix(inputDim,  outputDim))

}

object MLR {

  private[MLR] def softmax(x: Array[Double]): Unit = {
    val max = x.max
    var sum = 0.0

    for (i <- 0 until x.length) {
      x(i) = math.exp(x(i) - max)
      sum += x(i)
    }

    for (i <- 0 until x.length) x(i) /= sum
  }

  def train(data: RDD[(mutable.HashMap[Long, Double], Array[Double])], dm : DistML[Iterator[(Int, String, DataStore)]],
            maxIterations : Int, batchSize : Int): Unit = {

    val m = dm.model.asInstanceOf[MLRModel]
    val monitorPath = dm.monitorPath

    var lr = 0.1

    for (iter <- 0 to maxIterations) {
      val t = data.mapPartitionsWithIndex((index, it) => {

        println("Worker: tid=" + Thread.currentThread.getId + ", " + index)
        println("--- connecting to PS ---")
        val session = new Session(m, monitorPath, index)

        val weights = m.getMatrix("weights").asInstanceOf[DoubleMatrix]
        val samples = new util.LinkedList[(mutable.HashMap[Long, Double], Array[Double])]

        var cost = 0.0

        while(it.hasNext) {
          samples.clear()
          var count = 0
          while((count < batchSize) && it.hasNext) {
            samples.add(it.next())
            count += 1
          }
          val keys = new KeyList()
          for ((x, label) <- samples) {
            for (key <- x.keySet) {
              keys.addKey(key)
            }
          }
          val w = weights.fetch(keys, session)

          val w_old = new util.HashMap[Long, Array[Double]]
          for ((key, value) <- w) {
            var tmp:Array[Double] = new Array[Double](value.length)
            for (i <- 0 until value.length) {
              tmp(i) = value(i)
            }
            w_old.put(key, tmp)
          }

          for ((x, label) <- samples) {
            val p_y_given_x: Array[Double] = new Array[Double](m.outputDim)
            val dy: Array[Double] = new Array[Double](m.outputDim)

            val i:Long = 0
            for (i<-0 until m.outputDim) {
              p_y_given_x(i) = 0.0
              for (key <- x.keySet) {
                p_y_given_x(i) += (w.get(key))(i) * x.get(key).get
              }
            }

            softmax(p_y_given_x)

            for (i <- 0 until m.outputDim) {
              dy(i) = label(i) - p_y_given_x(i)
              for (key <- x.keySet) {
                (w.get(key)) (i.toInt) += lr * dy(i) * x.get(key).get
              }
              if (label(i) > 0.0) {
                cost = cost + label(i) * Math.log(p_y_given_x(i))
              }
            }
          }

          cost /= samples.size()

          // tuning in future
          for (key <- w.keySet()) {
            val grad: Array[java.lang.Double]  = new Array[java.lang.Double](m.outputDim)
            for (i <- 0 until m.outputDim) {
              grad(i) = w.get(key)(i) - w_old.get(key)(i)
            }
            w.put(key, grad)
          }

          weights.push(w, session)

          //session.progress(samples.size())
        }

        println("--- disconnect ---")
        session.disconnect()

        val r = new Array[Double](1)
        r(0) = cost
        r.iterator
      })

      dm.iterationDone()


      val totalCost = t.reduce(_+_)
      println("Total Cost: " + totalCost)
    }
  }

  def train(sc : SparkContext, samples: RDD[(mutable.HashMap[Long, Double], Array[Double])], psCount : Int, inputDim : Long, outputDim : Int,
            maxIterations : Int, batchSize : Int): DistML[Iterator[(Int, String, DataStore)]] = {

    val m = new MLRModel(inputDim, outputDim)

    val dm = DistML.distribute(sc, m, psCount, DistML.defaultF)
    val monitorPath = dm.monitorPath

    train(samples, dm, maxIterations, batchSize)

    dm
  }

  def validate(data : RDD[(mutable.HashMap[Long, Double], Array[Double])], dm : DistML[Iterator[(Int, String, DataStore)]]): Int = {
    val m = dm.model.asInstanceOf[MLRModel]
    val monitorPath = dm.monitorPath

    val t1 = data.mapPartitionsWithIndex( (index, it) => {
      val session = new Session(m, monitorPath, index)
      val weights = m.getMatrix("weights").asInstanceOf[DoubleMatrix]
      val w = weights.fetch(KeyCollection.ALL, session)

      var correct = 0
      var error = 0
      for((x, label) <- it) {
        val p_y_given_x: Array[Double] = new Array[Double](m.outputDim)
        val i:Long = 0
        for (i<-0 until m.outputDim) {
          p_y_given_x(i) = 0.0
          for (key <- x.keySet) {
            p_y_given_x(i) += (w.get(key))(i) * x.get(key).get
          }
          //add bias
        }

        softmax(p_y_given_x)
        val max = p_y_given_x.max
        var c_tmp = 0
        for (i <- 0 until m.outputDim) {
          if(label(i) > 0.0 && p_y_given_x(i) == max) {
            c_tmp = 1;
          }
        }
        if(c_tmp == 1) {
          correct += 1
        } else {
          error += 1
        }
      }

      session.disconnect()

      val r = new Array[Int](1)
      r(0) = correct
      //r(1) = error
      r.iterator
    })

    t1.reduce(_+_)
  }
}
