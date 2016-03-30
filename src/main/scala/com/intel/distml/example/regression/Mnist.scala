package com.intel.distml.example.regression

import java.util

import com.intel.distml.api.{Model, Session}
import com.intel.distml.platform.DistML
import com.intel.distml.regression.MLR
import com.intel.distml.util.{DoubleMatrix, KeyCollection, KeyList}
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

import scala.collection.mutable
import scala.collection.JavaConversions._

/**
  * Created by jimmy on 15-12-14.
  */
object Mnist {
  val BATCH_SIZE = 100
  private case class Params(
                          input: String = null,
                          inputDim: Long = 0,
                          outputDim: Int = 0,
                          partitions: Int = 1,
                          psCount: Int = 1,
                          batchSize: Int = 100,
                          maxIterations: Int = 500
                          )

  def main(args: Array[String]): Unit = {
    val mlrParams = Params()
    val parser = new OptionParser[Params]("MLRExample") {
      head("MLRExample: an example MLR(softmax) app for plain text data.")
      opt[Int]("inputDim")
        .text(s"dimensions of features. default: ${mlrParams.inputDim}")
        .action((x, c) => c.copy(inputDim = x))
      opt[Int]("outputDim")
        .text(s"dimensions of classification. default: ${mlrParams.outputDim}")
        .action((x, c) => c.copy(outputDim = x))
      opt[Int]("batchSize")
        .text(s"number of samples computed in a round. default: ${mlrParams.batchSize}")
        .action((x, c) => c.copy(batchSize = x))
      opt[Int]("psCount")
        .text(s"number of parameter servers. default: ${mlrParams.psCount}")
        .action((x, c) => c.copy(psCount = x))
      opt[Int]("partitions")
        .text(s"number of partitions for training data. default: ${mlrParams.partitions}")
        .action((x, c) => c.copy(partitions = x))
      opt[Int]("maxIterations")
        .text(s"number of iterations of training. default: ${mlrParams.maxIterations}")
        .action((x, c) => c.copy(maxIterations = x))
      arg[String]("<input>...")
        .text(s"input paths (directories) to plain text corpora." +
          s"Each text file line is one sample. default: ${mlrParams.input}")
        .unbounded()
        .required()
        .action((x, c) => c.copy(input = x))

    }

    parser.parse(args, mlrParams).map {
      params => run(params)
    }.getOrElse {
      parser.showUsageAsError
      sys.exit(1)
    }
  }

  def parseLine(line: String, dim: Long): Unit = {
    val items = line.split(" ")
    val labels = new Array[Double](dim.toInt)
    val len = items.length
    //val data = new Array[Double](len-1)
    val data = new util.HashMap[Long, Double]()
    for (i <- 0 to len-2) {
      val tmp = items(i).toDouble
      if (tmp != 0.0) {
        data(i) = tmp
      }
    }
    labels(items(len-1).toInt) = 1.0
    (data, labels)
  }



  def dumpweights(weights: util.HashMap[java.lang.Long, Array[java.lang.Double]], keyList: KeyList): Unit = {
    for (i<-0 until 2) {
      println("for line " + i)
      for (key <- keyList.iterator()) {
        print(" " + weights.get(key)(i))
      }
    }
  }

  def dumpweights(weights: util.HashMap[java.lang.Long, Array[java.lang.Double]], keySet: util.Set[Long]): Unit = {
    for (i<-0 until 2) {
      println("for line " + i)
      for (key <- keySet.iterator()) {
        print(" " + weights.get(key)(i))
      }
    }
  }
  def run(p: Params): Unit = {
    println("batchSize: " + p.batchSize)
    println("input: " + p.input)
    println("maxIterations: " + p.maxIterations)

    val conf = new SparkConf().setAppName("SparkMLR")

    val sc = new SparkContext(conf)
    val dim = p.outputDim
    val samples = sc.textFile(p.input).map(line => {
      val items = line.split(" ")
      val labels = new Array[Double](p.outputDim.toInt)
      val len = items.length
      //val data = new Array[Double](len-1)
      val data = new mutable.HashMap[Long, Double]()
      for (i <- 0 to len-2) {
        val tmp = items(i).toDouble
        if (tmp != 0.0) {
          data(i) = tmp
        }
      }
      labels(items(len-1).toInt) = 1.0
      (data, labels)
    }).repartition(p.partitions)

    val dm = MLR.train(sc, samples, p.psCount, p.inputDim, p.outputDim, p.maxIterations,  p.batchSize)

    val correct = MLR.validate(samples, dm)
    println("Total Correct " + correct)

    dm.recycle()
    sc.stop()
  }
}
