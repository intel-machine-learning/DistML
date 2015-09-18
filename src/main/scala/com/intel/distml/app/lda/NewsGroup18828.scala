package com.intel.distml.app.lda

import java.util.StringTokenizer

import com.intel.distml.api.{BigModelWriter, Model}
import com.intel.distml.model.lda.{Dictionary, LDADataMatrix, LDAModel, LDAModelWriter}
import com.intel.distml.platform.{TrainingContext, TrainingHelper}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * Created by yunlong on 9/4/15.
 */

object NewsGroup18828 {

  var docPath = "hdfs://dl-s1:9000/data/text/newsgroup_18828"
  var resultPath = "newsgroup_18828_results"
  val K = 10   //topic number
  val ITER = 100

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

    var rawDocs = spark.wholeTextFiles(docPath)
/*
    for (d <- rawDocs) {
      print("--------------=====================-----------------")
      print (d._1)
      print("<<<<<<<<<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>>>>>>")
      print (d._2)
      print("--------------=====================-----------------")
    }
*/
    var dic = new Dictionary()

    var words = rawDocs.flatMap(doc2Words).distinct().collect()
    words.foreach(x => dic.addWord(x))

    val bdic = spark.broadcast(dic)

    var rddTopic = rawDocs.mapPartitions(docs2LDAData(bdic))

    val config: TrainingContext = new TrainingContext();
    config.iteration(ITER);
    config.miniBatchSize(1);
    config.psCount(1);
    //    config.workerCount(1)

    val m: LDAModel = new LDAModel(0.5f, 0.1f, K, dic.getSize)

    val writer = new BigModelWriter();
    writer.setParamRowSize(LDAModel.MATRIX_PARAM_WORDTOPIC, K * 10);
    TrainingHelper.startTraining(spark, m, rddTopic, config, writer);
    System.out.println("LDA has ended!")

    val docs = rawDocs.collect()
    for (doc <- docs) {
      var t = m.inference(docs2IDs(dic)(doc), ITER)
      print("label:")
      for ( i <- 0 to t.length-1)
        print(" " + t(i))
      println()
    }

    spark.stop()
  }


  def doc2Words(doc: (String, String)) : Iterator[String] = {

    val lines = doc._2.lines

    val words = new ListBuffer[String]()
    while (lines.hasNext) {
      val line = lines.next();

      val strTok: StringTokenizer = new StringTokenizer(line)
      while (strTok.hasMoreTokens) {
        val token: String = strTok.nextToken
        words.append(token.toLowerCase.trim)
      }
    }
    words.iterator
  }

  def docs2LDAData(bdic : Broadcast[Dictionary])(docs : Iterator[(String, String)]) : Iterator[LDADataMatrix] = {

    var lst = ListBuffer[LDADataMatrix]();

    val dic = bdic.value

    while (docs.hasNext) {
      val lines = docs.next()._2.trim.lines;

      var wordIDs = new ListBuffer[Int]()
      val topics = new ListBuffer[Int]()
      val doctopic = new Array[Int](K)

      val words = new ListBuffer[String]()
      while (lines.hasNext) {
        val line = lines.next();

        val strTok: StringTokenizer = new StringTokenizer(line)
        while (strTok.hasMoreTokens) {
          val token: String = strTok.nextToken
          if (dic.contains(token)) {
            wordIDs += dic.getID(token)
            topics += Math.floor(Math.random()*K).toInt
          }
        }
      }

      topics.foreach(x => doctopic(x) = doctopic(x) + 1)

      lst += new LDADataMatrix(topics.toArray, wordIDs.toArray, doctopic)

    }
    lst.iterator
  }

  def docs2IDs(dic : Dictionary)(doc : (String, String)) : Array[Int] = {

      val lines = doc._2.trim.lines;

      var wordIDs = new ListBuffer[Int]()

      while (lines.hasNext) {
        val line = lines.next();

        val strTok: StringTokenizer = new StringTokenizer(line)
        while (strTok.hasMoreTokens) {
          val token: String = strTok.nextToken
          if (dic.contains(token)) {
            wordIDs += dic.getID(token)
          }
        }
      }

    wordIDs.toArray
  }

}
