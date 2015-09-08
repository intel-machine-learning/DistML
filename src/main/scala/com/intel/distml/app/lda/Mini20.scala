package com.intel.distml.app.lda

import com.intel.distml.api.databus.DataBus
import com.intel.distml.api.{BigModelWriter, Model}
import com.intel.distml.model.lda.{LDAModelWriter, Dictionary, LDADataMatrix, LDAModel}
import com.intel.distml.platform.{TrainingContext, TrainingHelper}
import com.intel.distml.util.Matrix
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * Created by yunlong on 9/4/15.
 */

object Mini20 {

  var trainFile = "hdfs://dl-s1:9000/data/text/mini20-train.txt"
  var testFile = "hdfs://dl-s1:9000/data/text/mini20-test.txt"
  val K = 20   //topic number

  var test = "comp.os.ms-windows.misc been run do for about month wa gener impress with the improv the multipl boot configur were great the new command were nice and doublespac work fine twice slow for larg data transfer twice fast for small with smartdrv until now thi morn while wa work research paper had reboot hung do program that did disk from within window when machin finish reboot found window directori and about two third other directori were irrevers corrupt cannot afford problem like thi return do mark also notic bad sector error from doublespac where none should exist"

  def removeLabel(src : String) : String = {
    val s = src.trim
    val i = s.indexOf('\t')
    if (i <= 0) s

    s.substring(i+1)
  }

  def getLabel(src : String) : String = {
    val s = src.trim
    val i = s.indexOf('\t')
    if (i <= 0) s

    s.substring(0, i)
  }

  def normalizeString(src : String) : String = {
    src.replaceAll("[^A-Z^a-z]", " ").trim().toLowerCase();
  }

  var dic = new Dictionary()

  def main(args: Array[String]) {

    var sparkMaster = args(0)
    var sparkHome = args(1)
    var sparkMem = args(2)
    var appJars = args(3)

    println("envir Info: spark master:"+sparkMaster+" spark home:"+sparkHome+" spark memo:"+sparkMem+" app jar:"+appJars)

    val conf = new SparkConf()
      .setMaster(sparkMaster)
      .setAppName("LDA")
      .set("spark.executor.memory", sparkMem)
      .set("spark.home", sparkHome)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setJars(Seq(appJars))

    val spark = new SparkContext(conf)
    Thread.sleep(3000)


    var rawLines = spark.textFile(trainFile).filter(s => s.trim().length > 0).map(removeLabel)
    println("====================the training file's lines number: " + rawLines.count() + " ==========")

    val words = rawLines.flatMap(line => line.split(" ")).filter(s => s.trim().length > 0).map(normalizeString).distinct().collect()
    words.foreach(x=>dic.addWord(x))

    println("====================the word number: " + words.size + " ==========")
    var rddTopic = rawLines.mapPartitions(transFromString2LDAData)

    val config: TrainingContext = new TrainingContext();
    config.iteration(100);
    config.miniBatchSize(2);
    config.psCount(1);
//    config.workerCount(1)

    val m: LDAModel = new LDAModel(0.5f, 0.1f, K, dic.getSize)

    //val writer = new BigModelWriter();
    //writer.setParamRowSize(LDAModel.MATRIX_PARAM_WORDTOPIC, K * 10);
    TrainingHelper.startTraining(spark, m, rddTopic, config, new LDAModelWriter(dic));
    System.out.println("LDA has ended!")

    predict(spark, m)

    spark.stop()

  }

  def transFromString2LDAData(itr : Iterator[String]) = {
    var lst = ListBuffer[LDADataMatrix]();

    while (itr.hasNext) {
      val line = itr.next().trim;
      val words = line.split(" ").iterator

      var wordIDs = new ListBuffer[Int]();
      words.foreach(x => wordIDs += dic.addWord(x))

      val topics = new ListBuffer[Int]()
      wordIDs.foreach(x => topics += Math.floor(Math.random()*K).toInt)

      val doctopic = new Array[Int](K)
      topics.foreach(x => doctopic(x) = doctopic(x) + 1)

      lst += new LDADataMatrix(topics.toArray, wordIDs.toArray, doctopic)

    }
    lst.iterator
  }

  def transFromString2IDs(itr : Iterator[String]) = {
    var lst = ListBuffer[Array[Int]]();

    while (itr.hasNext) {
      val line = itr.next().trim;
      val words = line.split(" ")

      var wordIDs = new ListBuffer[Int]();
      words.foreach(x => {
        val id = dic.getID(x)
        if (id != null)
          wordIDs += id
      })

      lst += wordIDs.toArray
    }
    lst.iterator
  }

  def predict(spark : SparkContext, model : LDAModel): Unit = {

    var rawLines = spark.textFile(testFile).filter(s => s.trim().length > 0)
    var labels = rawLines.map(getLabel).collect()
    var docs = rawLines.map(removeLabel).mapPartitions(transFromString2IDs).collect()

    var results = new Array[Int](docs.length)
    for (i <- 0 to docs.length - 1 ) {
      var topics = model.inference(docs(i), 100)
      results(i) = getMax(topics)
      println("result: " + results(i) + ", label: " + labels(i))
    }

  }

  def getMax(values : Array[Double]): Int = {
    var index = 0
    for (i <- 1 to values.length - 1) {
      if (values(index) < values(i)) {
        index = i
      }
    }

    index
  }

}
