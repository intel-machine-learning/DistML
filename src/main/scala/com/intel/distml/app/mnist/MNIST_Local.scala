package com.intel.distml.app.mnist

import com.intel.distml.api.Model
import com.intel.distml.platform.{TrainingContext, TrainingHelper}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by yunlong on 2/28/15.
 */
object MNIST_Local {
  @throws(classOf[InterruptedException])
  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setMaster("local[8]")
      .setAppName("MNIST_Local")

    val spark = new SparkContext(conf)
    Thread.sleep(3000)

//    val trainingFile: String = "hdfs://dl-s3:9000/test/mnist_train_1"
val trainingFile: String = "mnist_60.txt"
    val rawLines = spark.textFile(trainingFile)
    println(rawLines.count())
    val config: TrainingContext = new TrainingContext();
    val m: Model = new MNISTModel

    TrainingHelper.startTraining(spark, m, rawLines, config);
    System.out.println("digit recognition has ended!")


  }
}
