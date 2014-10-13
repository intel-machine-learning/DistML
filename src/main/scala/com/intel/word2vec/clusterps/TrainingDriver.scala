package com.intel.word2vec.clusterps

import java.util.Date
import org.apache.hadoop.fs.{PathFilter, Path, FileSystem}
import org.apache.spark.rdd.{UnionRDD, RDD}
import org.apache.spark.storage.StorageLevel
import scala.Double
import scala.Long
import scala.Predef._
import scala.collection.mutable
import scala.collection.mutable.{LinkedList, HashSet, HashMap}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark._
import scala.util.Random
import SparkContext._
import org.apache.hadoop.conf.Configuration
import java.net.{Socket, URI}
import java.io.{ObjectOutputStream, ObjectInputStream, DataOutputStream, DataInputStream}

import com.intel.word2vec.common._

/**
 * Created by He Yunlong on 7/16/14.
 */
object TrainingDriver {

  val MAX_CODE_LENGTH = 80
  val sample: Double = 1e-3
  var nextRandom: Long = 5
  var windowSize: Int = 5

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

  def createBinaryTree(lineCount : Long, alphabet : Array[(String, Long)]) : WordTree = {

    println("creating huffman tree: " + alphabet.length)

    var vocabSize = alphabet.length

    var r = new Random

    var neuronArray = new Array[HuffmanNode](vocabSize * 2)

    for (a <- 0 to vocabSize - 1) {
      neuronArray(a) = new HuffmanNode
      neuronArray(a).name = alphabet(a)._1
      neuronArray(a).index = a
      neuronArray(a).frequency = alphabet(a)._2
    }

    for (a <- vocabSize to neuronArray.length - 1) {
      neuronArray(a) = new HuffmanNode
      neuronArray(a).index = a
      neuronArray(a).frequency = 1000000000
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

    var t = new Array[WordNode](vocabSize)
    for (i <- 0 to t.length -1) {
      t(i) = new WordNode()
      t(i).initFrom(neuronArray(i))
      for (j <- 0 to t(i).codeLen -1) {
        if (t(i).point(j) >= vocabSize) {
          println("ERROR: index=" + i + ", point=" + t(i).point(j) + ", vocabSize=" + vocabSize)
        }
      }
    }

    return new WordTree(vocabSize, t, Constants.MODEL_DIMENSION)
  }

  def partitionTraining( driver : String,
                         servers : Array[ServerInfo],
                         expTableBroadcast : Broadcast[Array[Float]],
                         wordMapBroadcast : Broadcast[mutable.HashMap[String, Int]],
                         wordTreeBroadcast : Broadcast[WordTree],
                         totalWords : Long,
                         batchLines : Int,
                         trainThreadCount : Int)
                       (index : Int, lines: Iterator[String]) : Iterator[Int] = {

    val rt = Runtime.getRuntime();
    val usedMemory = rt.totalMemory() - rt.freeMemory();
    println("partition training index: " + index + ", useMemory=" + usedMemory)

    //var helper = new HDFSHelper(outputFolder)
    var dummy = new Array[Int](1)

    val globalExpTable = expTableBroadcast.value
    val wordMap = wordMapBroadcast.value
    val wordTree = wordTreeBroadcast.value

    val w = new Worker(index, driver, servers, globalExpTable, wordTree, wordMap, totalWords, lines,
      Constants.initialAlpha, batchLines, trainThreadCount)
    w.init()
    w.workNow()

    //helper.writeSubModel(wordTree, index)

    dummy.iterator
  }

  def normalizeString(src : String) : String = {

    //src.filter( c => ((c >= '0') && (c <= '9')) )
    src.filter( c => (((c >= 'a') && (c <= 'z')) || ((c >= 'A') && (c <= 'Z')) || (c == ' ')))

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

    expTable = new Array[Float](EXP_TABLE_SIZE)
    computeExp

    val opts = new scala.collection.mutable.HashMap[String, String]

    var sparkMaster = args(0)
    var sparkHome = args(1)
    var sparkMem = args(2)
    var appJars = args(3)
    val trainingFile = args(4)
    val trainingWords = args(5).equals("Yes")
    val outputFolder = args(6)

//    Constants.MIN_WORD_FREQ = Integer.parseInt(args(7))
//    Constants.MODEL_DIMENSION = Integer.parseInt(args(8))
//    Constants.BATCH_LINES = Integer.parseInt(args(9))
    var minFreq = Integer.parseInt(args(7))
    var batchLines = Integer.parseInt(args(8))
    val trainThreadCount = Integer.parseInt(args(9))

    val conf = new SparkConf()
      .setMaster(sparkMaster)
      .setAppName("Word2Vec")
      .set("spark.executor.memory", sparkMem)
      .set("spark.home", sparkHome)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //  .set("spark.driver.port", "44225")
     // .set("spark.local.dir", "/mnt/disk1/spark")
      .setJars(Seq("target/scala-2.10/word2vec_2.10-1.0.jar"))
    val spark = new SparkContext(conf)

    Thread.sleep(3000)  // waiting workers to register

    val rawLines = spark.textFile(trainingFile)
    var lines:RDD[String] = null
    if (trainingWords)
      lines = rawLines.map(normalizeString).persist(StorageLevel.MEMORY_AND_DISK)
    else
      lines = rawLines.persist(StorageLevel.MEMORY_AND_DISK)

    val words = lines.flatMap(line => line.split(" ")).filter( s => s.length > 0).map(word => (word, 1L))
    val lineCount = lines.count()
    println("lineCount=" + lineCount)

    val countedWords = words.reduceByKey(_ + _).map(exchange).filter(freqWordsOnly(minFreq)).sortByKey(false).map(exchange).collect
    println("countedWords=" + countedWords)

    var wordMap = new mutable.HashMap[String, Int]

    var totalWords = 0L
    for (i <- 0 to countedWords.length - 1) {
      var item = countedWords(i)
      wordMap.put(item._1, i)
      totalWords += item._2
    }
    println("totalWords=" + totalWords)
    var driver = Utils.getLocalIP()
    var wordTree = createBinaryTree(lineCount.toLong, countedWords)

    val socket = new Socket(driver, Constants.MONITOR_PORT)
    var dis = new DataInputStream(socket.getInputStream())
    var dos = new DataOutputStream(socket.getOutputStream())
    dos.writeInt(Constants.NODE_TYPE_DRIVER)
    dos.writeLong(totalWords)
    var oos = new ObjectOutputStream(socket.getOutputStream())
    oos.writeObject(wordTree)
    var psCount = IOHelper.readInt(dis)

    var psServers = new Array[ServerInfo](psCount)
    for (i <- 0 to psCount-1) {
      psServers(i) = new ServerInfo()
      psServers(i).address = dis.readUTF()
      psServers(i).serverIndex = IOHelper.readInt(dis)
      psServers(i).pushServicePort = IOHelper.readInt(dis)
      psServers(i).fetchServicePort = IOHelper.readInt(dis)

      println("param server: " + psServers(i).address + ", " + psServers(i).serverIndex)
    }
    println("all param servers are ready, start training now")

    var expTableBroadcast = spark.broadcast(expTable)
    var wordMapBroadcast = spark.broadcast(wordMap)
    var wordTreeBroadcast = spark.broadcast(wordTree)
    var startTime = System.currentTimeMillis()

    println("lineCount=" + lineCount)
    var dummy = lines.mapPartitionsWithIndex(
          partitionTraining(driver, psServers, expTableBroadcast, wordMapBroadcast, wordTreeBroadcast,
            totalWords, batchLines, trainThreadCount))
    dummy.saveAsTextFile(outputFolder + "/result")

    spark.stop()
  }
}
