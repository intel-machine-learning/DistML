package com.intel.distml.example

import java.util

import com.intel.distml.api.{Session, Model}
import com.intel.distml.platform.DistML
import com.intel.distml.util.{DoubleArray, KeyList}
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

import scala.collection.JavaConversions._

object SparseLR {

  private case class Params(
    psCount: Int = 2,
    input: String = null,
    dim: Long = 10000000000L,
    batchSize : Int = 100,
    maxIterations: Int = 100)

  def main(args: Array[String]) {

    val defaultParams = Params()

    val parser = new OptionParser[Params]("LDAExample") {
      head("LDAExample: an example LDA app for plain text data.")
      opt[Int]("dim")
        .text(s"dimension of features. default: ${defaultParams.dim}")
        .action((x, c) => c.copy(dim = x))
      opt[Int]("psCount")
        .text(s"number of parameter servers. default: ${defaultParams.psCount}")
        .action((x, c) => c.copy(psCount = x))
      opt[Int]("batchSize")
        .text(s"number of samples computed in a round. default: ${defaultParams.batchSize}")
        .action((x, c) => c.copy(batchSize = x))
      opt[Int]("maxIterations")
        .text(s"number of iterations of learning. default: ${defaultParams.maxIterations}")
        .action((x, c) => c.copy(maxIterations = x))
      arg[String]("<input>...")
        .text("input paths (directories) to plain text corpora." +
        "  Each text file line should hold 1 document.")
        .unbounded()
        .required()
        .action((x, c) => c.copy(input = x))
    }
    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      parser.showUsageAsError
      sys.exit(1)
    }
  }


  def run(p : Params): Unit = {

    println("batchSize: " + p.batchSize)
    println("input: " + p.input)
    println("maxIterations: " + p.maxIterations)

    val conf = new SparkConf().setAppName("SparseLR")
    val sc = new SparkContext(conf)

    val samples = sc.textFile(p.input).map( line => {
      val s = line.split(" ")
      val label = Integer.parseInt(s(0))

      val x = new util.HashMap[Long, Double]();
      for (i <- 1 to s.length-1) {
        val f = s(i).split(":")
        x.put(Integer.parseInt(f(0)), java.lang.Double.parseDouble(f(1)))
      }

      (x, label)
    })

    val m = new Model() {
      registerMatrix("weights", new DoubleArray(2000000L))
    }

    val dm = DistML.distribute(sc, m, p.psCount);
    val monitorPath = dm.monitorPath
    val eta = 0.1

    for (iter <- 0 to p.maxIterations) {
      println("============ Iteration: " + iter + " ==============")

      val t = samples.mapPartitionsWithIndex((index, it) => {

        println("--- connecting to PS ---")
        val session = new Session(m, monitorPath, index)
        val wd = m.getMatrix("weights").asInstanceOf[DoubleArray]

        val samples = new util.LinkedList[(util.HashMap[Long, Double], Int)]

//        var progress = 0
        var cost = 0.0

        while (it.hasNext) {
          samples.clear()
          var count = 0
          while ((count < p.batchSize) && it.hasNext) {
            samples.add(it.next())
            count = count + 1
          }

          val keys = new KeyList()

          for ((x, label) <- samples) {
            for (key <- x.keySet()) {
              keys.addKey(key)
            }
          }

          val w = wd.cache(keys, session)
          val w_old = new util.HashMap[Long, Double]
          for ((key, vlaue) <- w) {
            w_old.put(key, vlaue)
          }

          for ((x, label) <- samples) {
            var sum = 0.0
            for (key <- x.keySet()) {
              val value = x.get(key)
              sum = sum + w.get(key) * value
            }
            val h = 1.0 / (1.0 + Math.exp(-sum))

            val err = h - label
            for (key <- x.keySet) {
              val v: Double = w.get(key) - eta * err * x.get(key)
              w.put(key, v)
            }

            cost = cost + label * Math.log(h) + (1 - label) * Math.log(1 - h)
          }

//          cost /= samples.size()
//          progress = progress + samples.size()
//          println("progress: " + progress + ", cost: " + cost)

          for (key <- w.keySet) {
            val grad: Double = w.get(key) - w_old.get(key)
            w.put(key, grad)
          }

          wd.pushUpdates(w, session)
        }
        println("--- disconnect ---")
        session.disconnect()

        val r = new Array[Double](1)
        r(0) = cost
        r.iterator
      })

      val totalCost = t.reduce(_+_)
      println("Total Cost: " + totalCost)
    }

    dm.recycle()

    sc.stop();
  }
}
