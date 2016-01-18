package com.intel.distml.example

import java.io.File
import java.util
import java.util.Scanner

import com.intel.distml.api.{Session, Model}
import com.intel.distml.platform.DistML
import com.intel.distml.util.scala.DoubleArrayWithIntKey
import com.intel.distml.util.{DoubleArray, KeyList}
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object SparseLR {

  private case class Params(
    psCount: Int = 1,
    input: String = null,
    dim: Long = 10000000000L,
    eta: Double = 0.0001,
    batchSize : Int = 100,
    maxIterations: Int = 100 )

  def parseCancerData(line : String) : (mutable.HashMap[Int, Double], Int) = {
        var columns = line.split("\\s+")

        var x = new mutable.HashMap[Int, Double]()
        for (i <- 1 to 5) {
          x.put(i-1, Integer.parseInt(columns(i)))
        }
        var label = Integer.parseInt(columns(6))
        (x, label)
  }

  def parseBlanc(line : String) : (mutable.HashMap[Int, Double], Int) = {
    val s = line.split(" ")

    var label = Integer.parseInt(s(0))

    val x = new mutable.HashMap[Int, Double]();
    for (i <- 1 to s.length-1) {
      val f = s(i).split(":")
      val v = java.lang.Double.parseDouble(f(1))
      x.put(Integer.parseInt(f(0)), v)
    }

    (x, label)
  }

  def parseLR1(line : String) : (mutable.HashMap[Int, Double], Int) = {
    val s = line.split(",")

    var label = Integer.parseInt(s(s.length-1))

    val x = new mutable.HashMap[Int, Double]()
    var sum = 0.0
    for (i <- 0 to s.length - 2) {
      val v = java.lang.Double.parseDouble(s(i))
      x.put(i, v)
      sum += v * v
    }

    val r = Math.sqrt(sum)
    for (key <- x.keySet) {
      x.put(key, x(key)/r)
    }

    (x, label)
  }

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
      opt[Double]("eta")
        .text(s"learning rate. default: ${defaultParams.eta}")
        .action((x, c) => c.copy(eta = x))
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

  def write(model : Model) : Int = {
    println("total ps count: " + model.psCount);
    1
  }

  def run(p : Params): Unit = {

    println("batchSize: " + p.batchSize)
    println("input: " + p.input)
    println("maxIterations: " + p.maxIterations)

    val conf = new SparkConf().setAppName("SparseLR")
    val sc = new SparkContext(conf)

    val samples = sc.textFile(p.input).map(parseCancerData)

//    show(samples.collect())
    samples.repartition(1)

    val m = new Model() {
      registerMatrix("weights", new DoubleArrayWithIntKey(p.dim + 1))
    }

    val dm = DistML.distribute(sc, m, p.psCount, write)
    val monitorPath = dm.monitorPath

    for (iter <- 0 to p.maxIterations) {
      println("============ Iteration: " + iter + " ==============")

      val t = samples.mapPartitionsWithIndex((index, it) => {

        println("--- connecting to PS ---")
        val session = new Session(m, monitorPath, index)
        val wd = m.getMatrix("weights").asInstanceOf[DoubleArrayWithIntKey]

        val samples = new util.LinkedList[(mutable.HashMap[Int, Double], Int)]

        //var progress = 0
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
            for (key <- x.keySet) {
              keys.addKey(key)
            }
          }

          val w = wd.fetch(keys, session)

          val w_old = new util.HashMap[Long, Double]
          for ((key, value) <- w) {
            //println("w[" + key + "] = " + value)
            w_old.put(key, value)
          }

          for ((x, label) <- samples) {
            var sum = 0.0
            for ((k, v) <- x) {
              sum += w(k) * v
            }
            val h = 1.0 / (1.0 + Math.exp(-sum))

            val err = p.eta * (h - label)
            for ((k, v) <- x) {
              w.put(k, w(k) - err * v)
            }

            cost = cost + label * Math.log(h) + (1 - label) * Math.log(1 - h)
            //println("label: " + label + ", " + sum + ", " + h + ", " + Math.log(h) + ", " + Math.log(1-h) + ", cost: " + cost)
          }

          cost /= samples.size()
          //progress = progress + samples.size()
          //println("progress: " + progress + ", cost: " + cost)

          for (key <- w.keySet) {
            val grad: Double = w(key) - w_old(key)
            w.put(key, grad)
          }

          wd.push(w, session)
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

  def show(samples : Array[(util.HashMap[Long, Double], Int)]): Unit = {

    for (i <- 0 to samples.length-1) {
      val s = samples(i)
      for (key <- s._1.keySet()) {
        print ("" + key + " : " + s._1.get(key) + "\t")
      }
      println("[ " + s._2 + "]")
    }

  }
}
