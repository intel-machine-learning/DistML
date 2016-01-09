package com.intel.distml.example

import java.util
import com.intel.distml.api.{Session, Model}
import com.intel.distml.platform.DistML
import com.intel.distml.util.{KeyCollection, KeyList, DoubleMatrix, DoubleArray}
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser
import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Created by jimmy on 15-12-14.
  */
object MLR {
  val BATCH_SIZE = 100
  private case class Params(
                          input: String = null,
                          inputDim: Long = 0,
                          outputDim: Long = 0,
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

  def softmax(x: Array[Double]): Unit = {
    val max = x.max
    var sum = 0.0

    for (i <- 0 until x.length) {
      x(i) = math.exp(x(i) - max)
      sum += x(i)
    }

    for (i <- 0 until x.length) x(i) /= sum
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
    //conf.setMaster("spark://jimmy-Z97X-UD7-TH:7077")
    val sc = new SparkContext(conf)
    val dim = p.outputDim
    //val samples = sc.textFile(p.input).map(line => parseLinie(line, dim))
    val samples = sc.textFile(p.input).map(line => {
      val items = line.split(" ")
      val labels = new Array[Double](p.outputDim.toInt)
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
    })
    samples.repartition(1)
    val model = new Model() {
      //registerMatrix("weights", new DoubleMatrix( p.inputDim, p.outputDim.toInt))
      registerMatrix("weights", new DoubleMatrix(p.inputDim,  p.outputDim.toInt))
    }
    val dm = DistML.distribute(sc, model, 1)
    val mPath = dm.monitorPath
    var cost = 0.0
    var lr = 0.1

    for (iter <- 0 to p.maxIterations) {
      val t = samples.mapPartitionsWithIndex((index, it) => {
        println("--- connecting to PS ---")
        val session = new Session(model, mPath, index)
        val weights = model.getMatrix("weights").asInstanceOf[DoubleMatrix]
        val samples = new util.LinkedList[(util.HashMap[Long, Double], Array[Double])]


        while(it.hasNext) {
          samples.clear()
          var count = 0
          while((count < p.batchSize) && it.hasNext) {
            samples.add(it.next())
            count += 1
          }
          val keys = new KeyList()
          for ((x, label) <- samples) {
            for (key <- x.keySet()) {
              keys.addKey(key)
            }
          }
          val w = weights.fetch(keys, session)
          println("fetch from server")

          //dumpweights(w, keys)
          val w_old = new util.HashMap[Long, Array[Double]]
          for ((key, value) <- w) {
            var tmp:Array[Double] = new Array[Double](value.length)
            for (i <- 0 until value.length) {
              tmp(i) = value(i)
            }
            w_old.put(key, tmp)
          }

          //LR computing
          for ((x, label) <- samples) {
            val p_y_given_x: Array[Double] = new Array[Double](p.outputDim.toInt)
            val dy: Array[Double] = new Array[Double](p.outputDim.toInt)

            val i:Long = 0
            for (i<-0 until p.outputDim.toInt) {
              p_y_given_x(i) = 0.0
              for (key <- x.keySet()) {
                  p_y_given_x(i) += (w.get(key))(i) * x.get(key)
                  //println("w: " + (w.get(key))(i) + " x " + x.get(key)  )
                }
                //add bias
            }
            //softmax
            println("xxxxxxxxxxxxbefor soft max xxxxxxxxxxxxxxx")
            for (i <-0 until p_y_given_x.length)
              print(" " + p_y_given_x(i))
            println(" ")

            softmax(p_y_given_x)

            println("xxxxxxxx after softmax xxxxxxxxxxxxxxxxxxx")
            for (i <-0 until p_y_given_x.length)
              print(" " + p_y_given_x(i))
            println(" ")
            //gradiant
            for (i <- 0 until p.outputDim.toInt) {
              dy(i) = label(i) - p_y_given_x(i)
              for (key <- x.keySet()) {
                (w.get(key)) (i.toInt) += lr * dy(i) * x.get(key)
              }
              println(" ")
              //todo bias
              //deal cost
              if (label(i) > 0.0) {
                cost = cost + label(i) * Math.log(p_y_given_x(i))
                //println("Cost: " + cost)
                //println("Label: " + i)
                println("label is " + i + " py " + p_y_given_x(i))
              }
            }
              println("push to server")
            //dumpweights(w, keys)

          }
          println("Before Cost: " + cost)
          cost /= samples.size()
          println("After Cost: " + cost)
          // tuning in future
          for (key <- w.keySet()) {
            val grad: Array[java.lang.Double]  = new Array[java.lang.Double](p.outputDim.toInt)
            for (i <- 0 until p.outputDim.toInt) {
              grad(i) = w.get(key)(i) - w_old.get(key)(i)
            }
            w.put(key, grad)
          }
          //dumpweights(w, w.keySet())
          weights.push(w, session)
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

    val testRDD = sc.textFile(p.input).map(line => {
      val items = line.split(" ")
      val labels = new Array[Double](p.outputDim.toInt)
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
    })
    testRDD.repartition(1)
    val t1 = testRDD.mapPartitionsWithIndex( (index, it) => {
      val session = new Session(model, mPath, index)
      val weights = model.getMatrix("weights").asInstanceOf[DoubleMatrix]
      val w = weights.fetch(KeyCollection.ALL, session)
      var correct = 0
      var error = 0
      for((x, label) <- it) {
        val p_y_given_x: Array[Double] = new Array[Double](p.outputDim.toInt)
        val i:Long = 0
        for (i<-0 until p.outputDim.toInt) {
          p_y_given_x(i) = 0.0
          for (key <- x.keySet()) {
            p_y_given_x(i) += (w.get(key))(i) * x.get(key)
          }
          //add bias
        }

        softmax(p_y_given_x)
        val max = p_y_given_x.max
        var c_tmp = 0;
        for (i <- 0 until p.outputDim.toInt) {
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
      val r = new Array[Int](1)
      r(0) = correct
      //r(1) = error
      r.iterator
    })

    val correct = t1.reduce(_+_)
    println("Total Correct " + correct)
    dm.recycle()
    sc.stop()
  }
}
