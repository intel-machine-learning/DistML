package com.intel.distml.example.regression

import java.util

import com.intel.distml.api.Model
import com.intel.distml.platform.DistML
import com.intel.distml.util.scala.DoubleArrayWithIntKey
import com.intel.distml.util.store.DoubleArrayStore
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser

import scala.collection.mutable.HashMap

import com.intel.distml.regression.{LogisticRegression => LR}

/**
 * Created by yunlong on 3/11/16.
 */
object MelBlanc {

  private case class Params(
                             psCount: Int = 1,
                             input: String = null,
                             modelPath : String = null,
                             dim: Int = 10000000,
                             eta: Double = 0.0001,
                             partitions : Int = 1,
                             batchSize: Int = 100,
                             maxIterations: Int = 100
                             )

  def parseBlanc(line: String): (HashMap[Int, Double], Int) = {
    val s = line.split(" ")

    var label = Integer.parseInt(s(0))

    val x = new HashMap[Int, Double]();
    for (i <- 1 to s.length - 1) {
      val f = s(i).split(":")
      val v = java.lang.Double.parseDouble(f(1))
      x.put(Integer.parseInt(f(0)), v)
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
      opt[Int]("partitions")
        .text(s"number of partitions for training data. default: ${defaultParams.partitions}")
        .action((x, c) => c.copy(partitions = x))
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
        .text("path to train the model")
        .required()
        .action((x, c) => c.copy(input = x))
      arg[String]("<output>...")
        .text("path to save the model.")
        .required()
        .action((x, c) => c.copy(modelPath = x))
    }
    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      parser.showUsageAsError
      sys.exit(1)
    }
  }

  def run(p: Params): Unit = {

    println("batchSize: " + p.batchSize)
    println("input: " + p.input)
    println("maxIterations: " + p.maxIterations)

    val conf = new SparkConf().setAppName("SparseLR")
    val sc = new SparkContext(conf)

    val samples = sc.textFile(p.input).map(parseBlanc)
    val ratio = new Array[Double](2)
    ratio(0) = 0.9
    ratio(1) = 0.1
    val t = samples.randomSplit(ratio)
    val trainSet = t(0).repartition(p.partitions)
    val testSet = t(1)

    train(sc, trainSet, p.psCount, p.dim, 1, p.batchSize, p.modelPath)
    var auc = verify(sc, testSet, p.modelPath)
    println("auc: " + auc)

    trainAgain(sc, trainSet, p.maxIterations, p.batchSize, p.modelPath)
    auc = verify(sc, testSet, p.modelPath)
    println("auc: " + auc)

    sc.stop()
  }

  def train(sc : SparkContext, samples : RDD[(HashMap[Int, Double], Int)], psCount : Int, dim : Int, maxIterations : Int, batchSize : Int, modelPath : String): Unit = {
    val dm = LR.train(sc, samples, psCount, dim, maxIterations, batchSize)
    LR.save(dm, modelPath, "")
    dm.recycle()
  }

  def verify(sc : SparkContext, samples : RDD[(HashMap[Int, Double], Int)], modelPath : String): Double = {

    val dm = LR.load(sc, modelPath)

    val auc = LR.auc(samples, dm)

    dm.recycle()

    auc
  }

  def trainAgain(sc : SparkContext, samples : RDD[(HashMap[Int, Double], Int)], maxIterations : Int, batchSize : Int, modelPath : String): Unit = {
    val dm = LR.load(sc, modelPath)
    LR.train(samples, dm, maxIterations, batchSize)
    LR.save(dm, modelPath, "")
    dm.recycle()
  }

}
