package com.intel.distml.example.feature

import com.intel.distml.Dict
import com.intel.distml.api.{Session, Model}
import com.intel.distml.feature.Word2Vec
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.broadcast.Broadcast
import scopt.OptionParser

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * Created by yunlong on 16-3-23.
 */
object Word2VecExample {
  private case class Params(
                             psCount: Int = 1,
                             numPartitions : Int = 1,
                             input: String = null,
                             cbow: Boolean = true,
                             alpha: Double = 0.0f,
                             alphaFactor: Double = 10.0f,
                             window : Int = 7,
                             batchSize : Int = 100,
                             vectorSize : Int = 10,
                             minFreq : Int = 50,
                             maxIterations: Int = 100 ) {

    def show(): Unit = {
      println("=========== params =============")
      println("psCount: " + psCount)
      println("numPartitions: " + psCount)
      println("input: " + input)
      println("cbow: " + cbow)
      println("alpha: " + alpha)
      println("alphaFactor: " + alphaFactor)
      println("window: " + window)
      println("batchSize: " + batchSize)
      println("vectorSize: " + vectorSize)
      println("minFreq: " + minFreq)
      println("maxIterations: " + maxIterations)
      println("=========== params =============")
    }
  }

  def main(args: Array[String]) {

    val defaultParams = Params()

    val parser = new OptionParser[Params]("LDAExample") {
      head("LDAExample: an example LDA app for plain text data.")
      opt[Int]("psCount")
        .text(s"number of parameter servers. default: ${defaultParams.psCount}")
        .action((x, c) => c.copy(psCount = x))
      opt[Int]("numPartitions")
        .text(s"number of partitions for the training data. default: ${defaultParams.numPartitions}")
        .action((x, c) => c.copy(numPartitions = x))
      opt[Double]("alpha")
        .text(s"initiali learning rate. default: ${defaultParams.alpha}")
        .action((x, c) => c.copy(alpha = x))
      opt[Double]("alphaFactor")
        .text(s"factor to decrease adaptive learning rate. default: ${defaultParams.alphaFactor}")
        .action((x, c) => c.copy(alphaFactor = x))
      opt[Int]("batchSize")
        .text(s"number of samples computed in a round. default: ${defaultParams.batchSize}")
        .action((x, c) => c.copy(batchSize = x))
      opt[Int]("vectorSize")
        .text(s"vector size for a single word: ${defaultParams.vectorSize}")
        .action((x, c) => c.copy(vectorSize = x))
      opt[Int]("maxIterations")
        .text(s"number of iterations of learning. default: ${defaultParams.maxIterations}")
        .action((x, c) => c.copy(maxIterations = x))
      opt[Int]("minFreq")
        .text(s"minimum word frequency. default: ${defaultParams.minFreq}")
        .action((x, c) => c.copy(minFreq = x))
      opt[Boolean]("cbow")
        .text(s"true if use cbow, false if use skipgram. default: ${defaultParams.cbow}")
        .action((x, c) => c.copy(cbow = x))
      arg[String]("<input>...")
        .text("input paths (directories) to plain text corpora." +
        "  Each text file line should hold 1 document.")
        .unbounded()
        .required()
        .action((x, c) => c.copy(input = x))
    }
    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      parser.showUsageAsError
      sys.exit(1)
    }
  }

  def normalizeString(src : String) : String = {
    //src.filter( c => ((c >= '0') && (c <= '9')) )
    src.filter( c => (((c >= 'a') && (c <= 'z')) || ((c >= 'A') && (c <= 'Z')) || (c == ' '))).toLowerCase
  }

  def toWords(line : String) : Array[String] = {
    val words = line.split(" ").map(normalizeString)
    val list = new mutable.MutableList[String]()
    for (w <- words) {
      if (w.length > 0)
        list += w
    }

    list.toArray
  }

  def fromWordsToIds(bdic : Broadcast[Dict])(words : Array[String]) : Array[Int] = {

    val dic = bdic.value

    var wordIDs = new ListBuffer[Int]()

    for (w <- words) {
      val wn = normalizeString(w)
      if (dic.contains(wn)) {
        wordIDs.append(dic.getID(wn))
      }
    }

    wordIDs.toArray
  }

  def run(p : Params): Unit = {

    p.show

    val conf = new SparkConf().setAppName("Word2Vec")
    val sc = new SparkContext(conf)

    val rawLines = sc.textFile(p.input)
    val lines = rawLines.filter(s => s.length > 0).map(toWords).persist(StorageLevel.MEMORY_AND_DISK)

    val words = lines.flatMap(line => line.iterator).map((_, 1L))

    val countedWords = words.reduceByKey(_ + _).filter(f => f._2 > p.minFreq).sortBy(f => f._2).collect
    println("========== countedWords=" + countedWords.length + " ==================")

    var wordMap = new Dict

    var totalWords = 0L
    for (i <- 0 to countedWords.length - 1) {
      var item = countedWords(i)
      wordMap.put(item._1, i)
      totalWords += item._2
    }
    var wordTree = Word2Vec.createBinaryTree(countedWords)
    wordTree.tokens = totalWords

    var initialAlpha = 0.0f
    if (p.alpha < 10e-6) {
      if (p.cbow) {
        initialAlpha = 0.05f
      }
      else {
        initialAlpha = 0.0025f
      }
    }
    else {
      initialAlpha = p.alpha.toFloat
    }

    println("=============== Corpus Info Begin ================")
    println("Vocaulary: " + wordTree.vocabSize)
    println("Tokens: " + totalWords)
    println("Vector size: " + p.vectorSize)
    println("ps count: " + p.psCount)
    println("=============== Corpus Info End   ================")

    val bdic = sc.broadcast(wordMap)
    //var data = lines.map(fromWordsToIds(bdic)).repartition(1).persist(StorageLevel.MEMORY_AND_DISK)
    var data = lines.map(fromWordsToIds(bdic)).repartition(p.numPartitions).persist(StorageLevel.MEMORY_AND_DISK)

    var alpha = initialAlpha
    val dm = Word2Vec.train(sc, p.psCount, data, p.vectorSize, wordTree, initialAlpha, p.alphaFactor, p.cbow, p.maxIterations, p.window, p.batchSize)

    dm.recycle()
    val result = Word2Vec.collect(dm)

    val vectors = new Array[Array[Float]](result.size)
    for (w <- result) {
      vectors(w._1) = w._2
    }

    val blackIndex = wordMap.getID("black")
    val sims = Word2Vec.findSynonyms(vectors, vectors(blackIndex), 10)
    for (s <- sims) {
      println(wordMap.getWord(s._1) + ", " + s._2)
    }


    sc.stop()

    System.out.println("===== Finished ====")
  }

}
