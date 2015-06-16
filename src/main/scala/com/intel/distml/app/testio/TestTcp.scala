package com.intel.distml.app.testio

import java.io.{StringReader, BufferedReader}

import com.intel.distml.model.akka_tcp.TcpModel
import com.intel.distml.model.akka_tcp.PointSample
import com.intel.distml.platform.{TrainingHelper, TrainingConf}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lq on 6/16/15.
 */
object TestTcp {

  def main(args: Array[String]) {

    if (args.length == 0) {
      System.err.println("Usage: TestTcp <master> [<slices>]")
      System.exit(1)
    }

    var sparkMaster = args(0)
    var sparkHome = args(1)
    var sparkMem = args(2)
    var appJars = args(3)

    val conf = new SparkConf()
      .setMaster(sparkMaster)
      .setAppName("TestTcp")
      .set("spark.executor.memory", sparkMem)
      .set("spark.home", sparkHome)
    //  .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setJars(Seq(appJars))

    System.out.println("===== Run begin ====")

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

    var sampleRdd = spark.parallelize(samples, 3);

    val model: TcpModel = new TcpModel(6)

    val config = new TrainingConf().iteration(4);

    TrainingHelper.startTraining(spark, model, sampleRdd, config);

    spark.stop
    System.out.println("===== Run Done ====")
  }
}
