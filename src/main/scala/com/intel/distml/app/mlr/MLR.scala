package com.intel.distml.app.mlr

import java.io.{StringReader, BufferedReader}

import com.intel.distml.api.Model
import com.intel.distml.model.rosenblatt.{Rosenblatt, PointSample}
import com.intel.distml.model.sparselr.SparseLRModel
import com.intel.distml.platform.{TrainingHelper, TrainingConf}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by yunlong on 2/28/15.
 */
object MLR {
  val MLR_DIM: Int = 54

  @throws(classOf[InterruptedException])
  def main(args: Array[String]) {

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
    Thread.sleep(3000)

    val trainingFile: String = "hdfs://dl-s3:9000/test/covtype.scale.train.small"
    val rawLines = spark.textFile(trainingFile)
    val config: TrainingConf = new TrainingConf()
    val m: Model = new SparseLRModel(MLR_DIM)
    TrainingHelper.startTraining(spark, m, rawLines, config)
    System.out.println("digit recognition has ended!")
  }
}
