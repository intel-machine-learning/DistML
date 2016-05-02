package com.intel.distml.regression

import java.io.{DataInputStream, DataOutputStream}
import java.net.URI
import java.util
import java.util.Properties

import akka.actor.{ActorRef, ActorSystem}
import com.intel.distml.platform.{Clock, DistML, ParamServerDriver}
import com.intel.distml.util.store.DoubleArrayStore
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics

import scala.collection.JavaConversions._

import com.intel.distml.api.{Model, Session}
import com.intel.distml.util.{DataStore, KeyList}
import com.intel.distml.util.scala.DoubleArrayWithIntKey
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 * Created by yunlong on 3/10/16.
 */

object LogisticRegression {

  def load(sc: SparkContext, hdfsPath: String): DistML[Iterator[(Int, String, DataStore)]] = {

    // read meta
    val props = DistML.loadMeta(hdfsPath)

    val dim = Integer.parseInt(props.get("dim").asInstanceOf[String])
    val psCount = Integer.parseInt(props.get("psCount").asInstanceOf[String])

    // create distributed model and load parameters
    val m = new Model() {
      registerMatrix("weights", new DoubleArrayWithIntKey(dim))
    }

    val dm = DistML.distribute(sc, m, psCount, DistML.defaultF)
    val monitorPath = dm.monitorPath

    dm.load(hdfsPath)

    dm
  }

  def save(dm: DistML[Iterator[(Int, String, DataStore)]], hdfsPath: String, comments: String) {
    dm.save(hdfsPath)

    // save meta
    val m = dm.model
    val w = m.getMatrix("weights").asInstanceOf[DoubleArrayWithIntKey]

    val props = new Properties()
    props.put("dim", "" + w.getRowKeys.size())
    props.put("psCount", "" + dm.psCount)

    DistML.saveMeta(hdfsPath, props, comments)
  }

  def trainASGD(samples: RDD[(mutable.HashMap[Int, Double], Int)], dm : DistML[Iterator[(Int, String, DataStore)]],
            eta : Double, maxIterations : Int, batchSize : Int): Unit = {

    println("train ASGD with batch size: " + batchSize)
    val m = dm.model
    val monitorPath = dm.monitorPath

    dm.setTrainSetSize(samples.count())

    for (iter <- 0 to maxIterations - 1) {
      println("============ Iteration: " + iter + " ==============")

      val t = samples.mapPartitionsWithIndex((index, it) => {

        println("--- connecting to PS ---")
        val session = new Session(m, monitorPath, index)
        val wd = m.getMatrix("weights").asInstanceOf[DoubleArrayWithIntKey]
        val batch = new util.LinkedList[(mutable.HashMap[Int, Double], Int)]

        //var progress = 0
        var cost = 0.0
        val fetchClock = new Clock("Fetch")
        val trainClock = new Clock("Train")
        val pushClock = new Clock("Push")


        while (it.hasNext) {
          batch.clear()
          var count = 0
          while ((count < batchSize) && it.hasNext) {
            batch.add(it.next())
            count = count + 1
          }

          val keys = new KeyList()
          for ((x, label) <- batch) {
            for (key <- x.keySet) {
              keys.addKey(key)
            }
          }

          fetchClock.start()
          val w = wd.fetch(keys, session)
          fetchClock.stop()

          val w_old = new util.HashMap[Long, Double]
          for ((key, value) <- w) {
            w_old.put(key, value)
          }

          trainClock.start()
          for ((x, label) <- batch) {
            var sum = 0.0
            for ((k, v) <- x) {
              sum += w(k) * v
            }
            val h = 1.0 / (1.0 + Math.exp(-sum))

            val err = eta * (h - label)
            for ((k, v) <- x) {
              w.put(k, w(k) - err * v)
            }

            cost = cost + label * Math.log(h) + (1 - label) * Math.log(1 - h)
            //println("label: " + label + ", " + sum + ", " + h + ", " + Math.log(h) + ", " + Math.log(1-h) + ", cost: " + cost)
          }

          cost /= batch.size()
          //progress = progress + samples.size()
          //println("progress: " + progress + ", cost: " + cost)

          trainClock.stop()

          pushClock.start()
          for (key <- w.keySet) {
            val grad: Double = w(key) - w_old(key)
            w.put(key, grad)
          }

          wd.push(w, session)
          pushClock.stop()
        }

        println("--- disconnect ---")
        session.disconnect()

        val r = new Array[Double](1)
        r(0) = -cost
        r.iterator
      })

      val totalCost = t.reduce(_+_)
      println("============ Iteration done, Total Cost: " + totalCost + " ============")
    }

  }

  def trainSSP(samples: RDD[(mutable.HashMap[Int, Double], Int)], dm : DistML[Iterator[(Int, String, DataStore)]],
            eta : Double, maxIterations : Int, maxLag: Int): Unit = {

    println("train SSP with max lag: " + maxLag)
    val m = dm.model
    val monitorPath = dm.monitorPath

    dm.setTrainSetSize(samples.count())
    dm.startSSP(maxIterations, maxLag)

    val t = samples.mapPartitionsWithIndex((index, it) => {

      println("--- connecting to PS ---")
      val session = new Session(m, monitorPath, index)
      val wd = m.getMatrix("weights").asInstanceOf[DoubleArrayWithIntKey]

      val data = it.toArray


      val keys = new KeyList()
      for (item <- data) {
        for (key <- item._1.keySet) {
          keys.addKey(key)
        }
      }

      var cost = 0.0
      for (iter <- 0 to maxIterations - 1) {
        println("============ Iteration: " + iter + " ==============")
        val w = wd.fetch(keys, session)

        val w_old = new util.HashMap[Long, Double]
        for ((key, value) <- w) {
          w_old.put(key, value)
        }

        cost = 0.0
        for ((x, label) <- data) {
          var sum = 0.0
          for ((k, v) <- x) {
            sum += w(k) * v
          }
          val h = 1.0 / (1.0 + Math.exp(-sum))

          val err = eta * (h - label)
          for ((k, v) <- x) {
            w.put(k, w(k) - err * v)
          }

          cost = cost - (label * Math.log(h) + (1 - label) * Math.log(1 - h))
        }

        cost /= data.length

        for (key <- w.keySet) {
          val grad: Double = w(key) - w_old(key)
          w.put(key, grad)
        }

        wd.push(w, session)

        println("Iteration done: cost = " + cost)
        session.iterationDone(iter, cost)
      }

      println("--- disconnect ---")
      session.disconnect()

      val r = new Array[Double](1)
      r(0) = -cost
      r.iterator
    })

    val totalCost = t.reduce(_+_)
    println("============ Training done, Total Cost: " + totalCost + " ============")
  }

  def trainASGD(sc : SparkContext, samples: RDD[(mutable.HashMap[Int, Double], Int)], psCount : Int, dim : Long,
            eta : Double, maxIterations : Int, batchSize : Int): DistML[Iterator[(Int, String, DataStore)]] = {

    val m = new Model() {
      registerMatrix("weights", new DoubleArrayWithIntKey(dim + 1))
    }

    val dm = DistML.distribute(sc, m, psCount, DistML.defaultF)
    val monitorPath = dm.monitorPath

    trainASGD(samples, dm, eta, maxIterations, batchSize)

    dm
  }

  def trainASGD(sc : SparkContext, samples: RDD[(mutable.HashMap[Int, Double], Int)], psCount : Int,
                psBackup : Boolean, dim : Long,
                eta : Double, maxIterations : Int, batchSize : Int): DistML[Iterator[(Int, String, DataStore)]] = {

    val m = new Model() {
      registerMatrix("weights", new DoubleArrayWithIntKey(dim + 1))
    }

    val dm = DistML.distribute(sc, m, psCount, psBackup, DistML.defaultF)
    val monitorPath = dm.monitorPath

    trainASGD(samples, dm, eta, maxIterations, batchSize)

    dm
  }

  def trainSSP(sc : SparkContext, samples: RDD[(mutable.HashMap[Int, Double], Int)], psCount : Int, dim : Long,
            eta : Double, maxIterations : Int, maxLag : Int): DistML[Iterator[(Int, String, DataStore)]] = {

    val m = new Model() {
      registerMatrix("weights", new DoubleArrayWithIntKey(dim + 1))
    }

    val dm = DistML.distribute(sc, m, psCount, DistML.defaultF)
    val monitorPath = dm.monitorPath

    //    trainASGD(samples, dm, maxIterations, batchSize)
    trainSSP(samples, dm, eta, maxIterations, maxLag)

    dm
  }

  def collect(dm : DistML[Iterator[(Int, String, DataStore)]]): Array[(Int, Double)] = {
    val allWeights = dm.params().flatMap( it => {
      val m = it.next
      val store = m._3.asInstanceOf[DoubleArrayStore]
      val weights = store.iter()

      val result = new util.LinkedList[(Int, Double)]()
      while (weights.hasNext) {
        weights.next()
        result.add((weights.key().toInt, weights.value()))
      }

      result.iterator
    }).collect()

    allWeights
  }

  def predict(data : RDD[mutable.HashMap[Int, Double]], dm : DistML[Iterator[(Int, String, DataStore)]]): RDD[Double] = {

    val m = dm.model
    val result = data.mapPartitionsWithIndex((index, it) => {
      val session = new Session(m, dm.monitorPath, index)
      val wd = m.getMatrix("weights").asInstanceOf[DoubleArrayWithIntKey]

      val samples = new util.LinkedList[mutable.HashMap[Int, Double]]
      val labels = new util.LinkedList[Double]

      while (it.hasNext) {
        samples.add(it.next())
      }

      val keys = new KeyList()
      for (x <- samples) {
        for (key <- x.keySet) {
          keys.addKey(key)
        }
      }

      val w = wd.fetch(keys, session)
      for (x <- samples) {
        var sum = 0.0
        for ((k, v) <- x) {
          sum += w(k) * v
        }
        val h = 1.0 / (1.0 + Math.exp(-sum))
        labels.add(h)
      }

      session.disconnect();

      labels.iterator
    })

    result
  }

  def auc(data : RDD[(mutable.HashMap[Int, Double], Int)], dm : DistML[Iterator[(Int, String, DataStore)]]): Double = {

    val m = dm.model
    val monitorPath = dm.monitorPath

    val result = data.mapPartitionsWithIndex((index, it) => {
      val session = new Session(m, monitorPath, index)
      val wd = m.getMatrix("weights").asInstanceOf[DoubleArrayWithIntKey]

      val samples = new util.LinkedList[(mutable.HashMap[Int, Double], Int)]
      val labels = new util.LinkedList[(Double, Double)]

      while (it.hasNext) {
        samples.add(it.next())
      }

      val keys = new KeyList()
      for (x <- samples) {
        for (key <- x._1.keySet) {
          keys.addKey(key)
        }
      }

      val w = wd.fetch(keys, session)
      for ((x, label) <- samples) {
        var sum = 0.0
        for ((k, v) <- x) {
          sum += w(k) * v
        }
        val h = 1.0 / (1.0 + Math.exp(-sum))
        labels.add((h, label.toDouble))
      }

      session.disconnect();

      labels.iterator
    })

    val metrics = new BinaryClassificationMetrics(result)
    metrics.areaUnderROC()
  }

}
