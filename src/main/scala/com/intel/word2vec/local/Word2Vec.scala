package com.intel.word2vec.local

import java.io.FileOutputStream
import java.net.URI
import java.nio.ByteBuffer
import java.util.Date
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{PathFilter, Path, FileSystem}
import org.apache.spark.rdd.{UnionRDD, RDD}
import org.apache.spark.storage.StorageLevel
import scala.Double
import scala.Long
import scala.Predef._
import scala.collection.mutable
import scala.collection.mutable.{HashSet, HashMap}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark._
import scala.util.Random
import SparkContext._


/**
 * Created by He Yunlong on 4/24/14.
 */
object Word2Vec {

  val MAX_CODE_LENGTH = 40
  val MIN_WORD_FREQ = 50

  val sample: Double = 1e-3
  var nextRandom: Long = 5
  var windowSize: Int = 5
  var vectorSize: Int = 200

  var expTable: Array[Float] = null
  final val EXP_TABLE_SIZE: Int = 1000
  final val MAX_EXP: Int = 6

  private def computeExp {
    expTable = new Array[Float](EXP_TABLE_SIZE)
    var i: Int = 0
    for (i <- 0 to EXP_TABLE_SIZE-1) {
      expTable(i) = math.exp(((i / EXP_TABLE_SIZE.asInstanceOf[Float] * 2 - 1) * MAX_EXP)).toFloat
      expTable(i) = expTable(i) / (expTable(i) + 1)
    }
  }

  def createBinaryTree(lineCount : Long, alphabet : Array[(String, Long)]) : Array[NeuronNode] = {

    println("creating huffman tree: " + alphabet.length)

    var vocabSize = alphabet.length + 1;

    var r = new Random
    var neuronArray = new Array[NeuronNode](vocabSize * 2 + 1)

    neuronArray(0) = new NeuronNode
    //println("created")
    neuronArray(0).name = "<s>"
    neuronArray(0).index = 0
    neuronArray(0).frequency = lineCount
    //neuronArray(0).initData()

    for (a <- 1 to vocabSize - 1) {
      //println("creating neuron: " + a)
      neuronArray(a) = new NeuronNode
      //println("created")
      neuronArray(a).name = alphabet(a-1)._1
      neuronArray(a).index = a
      neuronArray(a).frequency = alphabet(a-1)._2
    }

    for (a <- vocabSize to neuronArray.length - 1) {
      neuronArray(a) = new NeuronNode
      neuronArray(a).index = a
      neuronArray(a).frequency = 1000000000
//      neuronArray(a).initVec(r)
    }

    var pos1 = vocabSize-1
    var pos2 = vocabSize
    var min1i, min2i : Int = 0

    for (a <- 0  to vocabSize-2) {
      if (pos1 >= 0) {
        if (neuronArray(pos1).frequency < neuronArray(pos2).frequency) {
          min1i = pos1
          pos1 -= 1
        } else {
          min1i = pos2
          pos2 += 1
        }
      } else {
        min1i = pos2
        pos2 += 1
      }
      if (pos1 >= 0) {
        if (neuronArray(pos1).frequency < neuronArray(pos2).frequency) {
          min2i = pos1
          pos1 -= 1
        } else {
          min2i = pos2
          pos2 += 1
        }
      } else {
        min2i = pos2
        pos2 +=1
      }

      neuronArray(vocabSize + a).frequency =
        neuronArray(min1i).frequency +
          neuronArray(min2i).frequency

      neuronArray(min1i).parentIndex = vocabSize + a
      neuronArray(min2i).parentIndex = vocabSize + a

      neuronArray(min2i).binary = 1
    }

    // Now assign binary code to each vocabulary word
    var b : Int = 0
    var i : Int = 0
    var code = new Array[Int](MAX_CODE_LENGTH)
    var point = new Array[Int](MAX_CODE_LENGTH)

    for (a <- 0  to vocabSize-1) {
      b = a
      i = 0
      var reachEnd = false
      while (!reachEnd) {
        code(i) = neuronArray(b).binary
        point(i) = b
        i += 1
        b = neuronArray(b).parentIndex
        if (b == vocabSize *2 - 2)
          reachEnd = true
      }
      neuronArray(a).codeLen = i
      neuronArray(a).point(0) = vocabSize - 2
      for (b <- 0 to i-1) {
        neuronArray(a).code(i - b - 1) = code(b)
        neuronArray(a).point(i - b) = point(b) - vocabSize
      }
    }

    var t = new Array[NeuronNode](vocabSize)
    for (i <- 0 to t.length -1) {
      t(i) = neuronArray(i)
    }

    t
  }

  def normalizeString(src : String) : String = {

    //src.filter( c => ((c >= '0') && (c <= '9')) )
    src.filter( c => (((c >= 'a') && (c <= 'z')) || ((c >= 'A') && (c <= 'Z')) || (c == ' ')))

  }

  def exchange[T, U](w : (T, U)) : (U, T) = {
    (w._2, w._1)
  }

  def freqWordsOnly(W : (Long, String)) : Boolean = {
    W._1 > MIN_WORD_FREQ
  }


  def main(args: Array[String]) {

    if (args.length == 0) {
      System.err.println("Usage: SparkPi <master> [<slices>]")
      System.exit(1)
    }

    expTable = new Array[Float](EXP_TABLE_SIZE)
    computeExp

    val opts = new scala.collection.mutable.HashMap[String, String]
    var sparkMaster = args(0)
    var sparkHome = args(1)
    var sparkMem = args(2)
    var appJars = args(3)
    val trainingFile = args(4)
    val outputFolder = args(5)
    val paramServer = args(6)
    val pushStep = Integer.parseInt(args(7))
    val fetchStep = Integer.parseInt(args(8))

    val spark = new SparkContext(sparkMaster, "Word2Vec", sparkHome, appJars.split(":").toSeq, opts)
    System.setProperty("spark.serializer", "spark.KryoSerializer")

    System.out.println("create rdd for reading words from hdfs")
    val lines = spark.textFile(trainingFile).map(normalizeString).persist(StorageLevel.MEMORY_AND_DISK)
    //val lines = spark.textFile(trainingFile) //.cache()
    //val lines = spark.textFile(trainingFile).persist(StorageLevel.MEMORY_AND_DISK)
    val words = lines.flatMap(line => line.split(" ")).filter( s => s.length > 0).map(word => (word, 1L))
    val lineCount = lines.count()
    println("lineCount=" + lineCount)

    val countedWords = words.reduceByKey(_ + _).map(exchange).filter(freqWordsOnly).sortByKey(false).map(exchange).collect
    println("countedWords=" + countedWords)

    var wordMap = new mutable.HashMap[String, Int]

    var totalWords = 0L
    wordMap.put("<s>", 0)
    for (i <- 0 to countedWords.length - 1) {
      var item = countedWords(i)
      wordMap.put(item._1, i+1)
      totalWords += item._2
    }
    println("totalWords=" + totalWords)
    var wordTree = createBinaryTree(lineCount.toLong, countedWords)

    var startTime = System.currentTimeMillis()

    val w = new Worker(expTable, wordTree, wordMap, totalWords, outputFolder)
    w.trainWords(lines.toLocalIterator)

    spark.stop()
  }
}
