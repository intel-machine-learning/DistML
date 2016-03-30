package com.intel.distml.example.clustering

import com.intel.distml.Dict
import com.intel.distml.api.Model
import com.intel.distml.clustering.{LightLDA, LDAParams}
import com.intel.distml.platform.DistML
import com.intel.distml.util.{IntMatrixWithIntKey, IntArrayWithIntKey}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser

import scala.collection.mutable.ListBuffer

/**
 * Created by yunlong on 16-3-29.
 */
object LDAExample {

  def normalizeString(src : String) : String = {
    src.replaceAll("[^A-Z^a-z]", " ").trim().toLowerCase();
  }

  def fromWordsToIds(bdic : Broadcast[Dict])(line : String) : Array[Int] = {

    val dic = bdic.value

    val words = line.split(" ")

    var wordIDs = new ListBuffer[Int]();

    for (w <- words) {
      val wn = normalizeString(w)
      if (dic.contains(wn)) {
        wordIDs.append(dic.getID(wn))
      }
    }

    wordIDs.toArray
  }

  def main(args: Array[String]) {

    val defaultParams = new LDAParams()

    val parser = new OptionParser[LDAParams]("LDAExample") {
      head("LDAExample: an example LDA app for plain text data.")
      opt[Int]("k")
        .text(s"number of topics. default: ${defaultParams.k}")
        .action((x, c) => c.copy(k = x))
      opt[Int]("batchSize")
        .text(s"number of samples used in one computing. default: ${defaultParams.batchSize}")
        .action((x, c) => c.copy(batchSize = x))
      opt[Int]("psCount")
        .text(s"number of parameter servers. default: ${defaultParams.psCount}")
        .action((x, c) => c.copy(psCount = x))
      opt[Double]("alpha")
        .text(s"super parameter for sampling. default: ${defaultParams.alpha}")
        .action((x, c) => c.copy(alpha = x))
      opt[Double]("beta")
        .text(s"super parameter for sampling. default: ${defaultParams.beta}")
        .action((x, c) => c.copy(beta = x))
      opt[Int]("maxIterations")
        .text(s"number of iterations of learning. default: ${defaultParams.maxIterations}")
        .action((x, c) => c.copy(maxIterations = x))
      opt[Int]("partitions")
        .text(s"number of partitions to train the model. default: ${defaultParams.partitions}")
        .action((x, c) => c.copy(partitions = x))
      opt[Boolean]("showPlexity")
        .text(s"Show plexity after each iteration." +
        s" default: ${defaultParams.showPlexity}")
        .action((x, c) => c.copy(showPlexity = x))
      arg[String]("<input>...")
        .text("input paths (directories) to plain text corpora.")
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

  private def run(p: LDAParams) {
    val conf = new SparkConf().setAppName(s"DistML.Example.LDA")
    conf.set("spark.driver.maxResultSize", "5g")

    val sc = new SparkContext(conf)
    Thread.sleep(3000)

    var rawLines = sc.textFile(p.input).filter(s => s.trim().length > 0)

    var dic = new Dict()

    val words = rawLines.flatMap(line => line.split(" ")).map(normalizeString).filter(s => s.trim().length > 0).distinct().collect()
    words.foreach(x => dic.addWord(x))

    p.init(dic.getSize)

    val bdic = sc.broadcast(dic)
    var data = rawLines.map(fromWordsToIds(bdic)).map(ids => {
      //      println("=============== random initializing start ==================")
      val topics = new ListBuffer[(Int, Int)]
      ids.foreach(x => topics.append((x, Math.floor(Math.random() * p.k).toInt)))

      val doctopic = new Array[Int](p.k)
      topics.foreach(x => doctopic(x._2) = doctopic(x._2) + 1)

      //      println("=============== random initializing done ==================")
      (doctopic, topics.toArray)
    }).repartition(p.partitions).persist(StorageLevel.MEMORY_AND_DISK)

    var statistics = data.map(d => (1, d._2.length)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))

    println("=============== Corpus Info Begin ================")
    println("Vocaulary: " + dic.getSize)
    println("Docs: " + statistics._1)
    println("Tokens: " + statistics._2)
    println("Topics: " + p.k)
    println("=============== Corpus Info End   ================")


    val dm = LightLDA.train(sc, data, dic.getSize, p)

    dm.recycle()
    sc.stop
  }
}
