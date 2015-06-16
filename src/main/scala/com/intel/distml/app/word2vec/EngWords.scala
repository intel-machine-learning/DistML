package com.intel.distml.app.word2vec

import com.intel.distml.api.Model
import com.intel.distml.model.word2vec.Word2VecModel
import com.intel.distml.util.Matrix1D
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}
import com.intel.distml.platform.{TrainingConf, TrainingHelper}

import scala.collection.mutable
import scala.util.Random
import java.net._
import org.apache.hadoop.fs.{FSDataOutputStream, PathFilter, Path, FileSystem}
import java.io.{DataOutputStream, ObjectInputStream, DataInputStream}

import org.apache.hadoop.conf.Configuration

object EngWords {

  def normalizeString(src : String) : String = {

    //src.filter( c => ((c >= '0') && (c <= '9')) )
    src.filter( c => (((c >= 'a') && (c <= 'z')) || ((c >= 'A') && (c <= 'Z')) || (c == ' '))).toLowerCase

  }

  def exchange[T, U](w : (T, U)) : (U, T) = {
    (w._2, w._1)
  }

  def freqWordsOnly(minFreq : Int)(W : (Long, String)) : Boolean = {
    W._1 > minFreq
  }


  def main(args: Array[String]) {

    if (args.length == 0) {
      System.err.println("Usage: SparkPi <master> [<slices>]")
      System.exit(1)
    }

    var sparkMaster = args(0)
    var sparkHome = args(1)
    var sparkMem = args(2)
    var appJars = args(3)
    val trainingFile ="/media/lq/Data/backup/eng_news_2010_1M-co_n.txt"// "hdfs://dl-s1:9000/data/text/eng_news_256m"
    val outputFolder = "/media/lq/Data/backup/"//hdfs://dl-s3:9000/user/yunlong/word2vec"
    //      val trainingFile = "file:/home/harry/workspace/scaml/novel.txt"

    System.setProperty("spark.driver.maxResultSize", "1g");
    val conf = new SparkConf()
      .setMaster(sparkMaster)
      .setAppName("Word2Vec")
      .set("spark.executor.memory", sparkMem)
      .set("spark.home", sparkHome)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setJars(Seq(appJars))

    val spark = new SparkContext(conf)

    Thread.sleep(3000) // waiting workers to register

    val rawLines = spark.textFile(trainingFile)
    var lines: RDD[String] = null
    //if (trainingWords)
    lines = rawLines.map(normalizeString).filter(s => s.length > 0).persist(StorageLevel.MEMORY_AND_DISK)
    //else
    //lines = rawLines.persist(StorageLevel.MEMORY_AND_DISK)

    val words = lines.flatMap(line => line.split(" ")).filter(s => s.length > 0).map(word => (word, 1L))
    val lineCount = lines.count()
    println("lineCount=" + lineCount)

    val countedWords = words.reduceByKey(_ + _).map(exchange).filter(freqWordsOnly(Word2VecModel.minFreq)).sortByKey(false).map(exchange).collect
    println("========== countedWords=" + countedWords.length + " ==================")

    var wordMap = new mutable.HashMap[String, Int]

    var totalWords = 0L
    for (i <- 0 to countedWords.length - 1) {
      var item = countedWords(i)
      wordMap.put(item._1, i)
      totalWords += item._2
    }
    var wordTree = Word2VecModel.createBinaryTree(lineCount.toLong, countedWords)

//    val config = new TrainingConf().psCount(2).groupCount(6).miniBatchSize(1000)
    val config = new TrainingConf().miniBatchSize(1000)

    val model = new Word2VecModel(wordTree, wordMap, 200)

    TrainingHelper.startTraining(spark, model, rawLines, config)
    val w2vApi = Word2VecModel.getWord2VecMap(model)
    val synonyms = w2vApi.findSynonyms("man", 20)

    for((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
    }


    spark.stop
    System.out.println("===== Run Done ====")

    Word2VecModel.saveToHDFS(outputFolder, model)

    System.out.println("===== Finished ====")
  }

}
