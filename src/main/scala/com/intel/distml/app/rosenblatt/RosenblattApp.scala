package com.intel.distml.app.rosenblatt

import java.io.{BufferedReader, StringReader}

import com.intel.distml.model.rosenblatt.{Rosenblatt, PointSample}
import com.intel.distml.platform.{TrainingHelper, TrainingContext}
import org.apache.spark.{SparkContext, SparkConf}

import scala.util.Random

/**
 * Created by yunlong on 2/4/15.
 */
object RosenblattApp {

  def main(args: Array[String]) {

    if (args.length == 0) {
      System.err.println("Usage: RosenblattApp <master> [<slices>]")
      System.exit(1)
    }

    var sparkMaster = args(0)
    var sparkHome = args(1)
    var sparkMem = args(2)
    var appJars = args(3)

    val conf = new SparkConf()
      .setMaster(sparkMaster)
      .setAppName("Word2Vec")
      .set("spark.executor.memory", sparkMem)
      .set("spark.home", sparkHome)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setJars(Seq(appJars))

    val spark = new SparkContext(conf)

    Thread.sleep(3000)  // waiting workers to register

    val data =
      """1,0,1,0,0,0,1
        1,0,1,1,0,0,1
        1,0,1,0,1,0,1
        1,1,0,0,1,1,1
        1,1,1,1,0,0,1
        1,1,1,1,0,0,1
        1,0,0,0,1,1,1
        1,0,0,0,1,0,0
        0,1,1,1,0,1,1
        0,1,1,0,1,1,0
        0,0,0,1,1,0,0
        0,1,0,1,0,1,0
        0,0,0,1,0,1,0
        0,1,1,0,1,1,0
        0,1,1,1,0,0,0
      """.stripMargin.trim

    val reader = new BufferedReader(new StringReader(data));

    var samples = new Array[PointSample](15)
    var line = "";
    for(i <- 0 to samples.length-1) {
      line = reader.readLine.trim
      val values = line.split(",");
      val x = new Array[Double](6)
      for (j <- 0 to 5) {
        x(j) = Integer.parseInt(values(j));
      }
      val label = Integer.parseInt(values(6));
      samples(i) = new PointSample(x, label);
    }

    var sampleRdd = spark.parallelize(samples, 1);

    val model: Rosenblatt = new Rosenblatt(6)

    val config = new TrainingContext().iteration(4);

    TrainingHelper.startTraining(spark, model, sampleRdd, config)

    model.showResult()

    spark.stop
    System.out.println("===== Run Done ====")
  }
}
