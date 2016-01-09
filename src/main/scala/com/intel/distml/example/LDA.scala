package com.intel.distml.example

import java.io.Serializable
import java.util

import scopt.OptionParser

import _root_.scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._

import com.intel.distml.api.{Session, Model}
import com.intel.distml.platform.DistML
import com.intel.distml.util._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by yunlong on 12/10/15.
 */
class LDA {

  private case class Params(
    psCount: Int = 2,
    input: String = null,
    k: Int = 20,
    maxIterations: Int = 10,
    showPlexity: Boolean = true)

  var dic = new Dict()
  val K = 20000 //topic number

  val BATCH_SIZE = 100
  val ITERATIONS = 100

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

    val defaultParams = Params()

    val parser = new OptionParser[Params]("LDAExample") {
      head("LDAExample: an example LDA app for plain text data.")
      opt[Int]("k")
        .text(s"number of topics. default: ${defaultParams.k}")
        .action((x, c) => c.copy(k = x))
      opt[Int]("psCount")
        .text(s"number of parameter servers. default: ${defaultParams.psCount}")
        .action((x, c) => c.copy(psCount = x))
      opt[Int]("maxIterations")
        .text(s"number of iterations of learning. default: ${defaultParams.maxIterations}")
        .action((x, c) => c.copy(maxIterations = x))
      opt[Boolean]("showPlexity")
        .text(s"Show plexity after each iteration." +
        s" default: ${defaultParams.showPlexity}")
        .action((x, c) => c.copy(showPlexity = x))
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

  private def run(p: Params) {
    val conf = new SparkConf().setAppName(s"DistML.Example.LDA")
    conf.set("spark.driver.maxResultSize", "5g")

    val sc = new SparkContext(conf)

    var rawLines = sc.textFile(p.input).filter(s => s.trim().length > 0).map(removeLabel)

    val words = rawLines.flatMap(line => line.split(" ")).filter(s => s.trim().length > 0).map(normalizeString).distinct().collect()
    words.foreach(x=>dic.addWord(x))

    val bdic = sc.broadcast(dic)
    var docs = rawLines.map( fromWordsToIds(bdic)).map(ids => {
      val topics = new ListBuffer[Int]()
      ids.foreach(x => topics += Math.floor(Math.random()*K).toInt)

      val doctopic = new Array[Int](K)
      topics.foreach(x => doctopic(x) = doctopic(x) + 1)

      (ids, topics.toArray, doctopic)
    })

    val m = new Model() {
      registerMatrix("doc-topics", new IntArray(K))
      registerMatrix("word-topics", new IntMatrix(dic.getSize, K))
    }

    val dm = DistML.distribute(sc, m, p.psCount);
    val monitorPath = dm.monitorPath

    // first round, init parameters
    val t = docs.mapPartitionsWithIndex((index, it) => {
      println("--- connecting to PS ---")
      val session = new Session(m, monitorPath, index)
      val dtm = m.getMatrix("doc-topics").asInstanceOf[IntArray]
      val wtm = m.getMatrix("word-topics").asInstanceOf[IntMatrix]

      while (it.hasNext) {

        val (ids, topics, doctopic) = it.next()

        val keys = new KeyList()
        for (w <- ids) {
          keys.addKey(w)
        }

        val dt = dtm.fetch(KeyCollection.ALL, session)
        val dt_old = new util.HashMap[Long, Int]
        for (i <- 0 to K-1) {
          dt_old.put(i, dt.get(i))
        }

        val wt = wtm.fetch(keys, session)
        val wt_old = new util.HashMap[Long, Array[Int]]
        for ((key, value) <- wt) {
          val ts = new Array[Int](value.length)
          for (i <- 0 to value.length-1) ts(i) = value(i)
          wt_old.put(key, ts)
        }

        for (i <- 0 to ids.length - 1) {
          val wId = ids(i)
          val topic = topics(i)

          val wts = wt.get(wId)
          wts(topic) = wts(topic) + 1

          val dtc = dt.get(topic)
          dt.put(topic.toLong, dtc + 1)
        }

        dtm.push(dt, session)
        //wtm.pushUpdates(wt, de)
      }
      val r = new Array[Int](1)
      r.iterator

    })

    // invoke the job
    println(t.collect().length)

    for (iter <- 0 to ITERATIONS) {
      val alpha = 0.01
      val beta = 0.01
      val V = dic.getSize

      docs = docs.mapPartitionsWithIndex( (index, it) => {
        val result = new ListBuffer[(Array[Int], Array[Int], Array[Int])]

        println("--- connecting to PS ---")
        val session = new Session(m, monitorPath, index)
        val dtm = m.getMatrix("doc-topics").asInstanceOf[IntArrayWithIntKey]
        val wtm = m.getMatrix("word-topics").asInstanceOf[IntMatrixWithIntKey]
        val p = new Array[Double](K)

        while (it.hasNext) {

          val doc = it.next()

          val wkeys = new KeyList
          for (wid <- doc._1) {
            wkeys.addKey(wid)
          }

          val dt = dtm.fetch(KeyCollection.ALL, session)
          val wt = wtm.fetch(wkeys, session)

          result += sampling(doc, wt, dt, p, alpha, beta, V)

          dtm.push(dt, session)
          //wtm.pushUpdates(wt, de)
        }

        result.iterator
      })
    }
  }

  def hashMapToArray(d : java.util.HashMap[java.lang.Long, Integer]) : Array[Integer] = {
    val a = new Array[Integer](d.size())
    for ( entry <- d.entrySet()) {
      a(entry.getKey.toInt) = entry.getValue
    }

    a
  }

  def sampling(doc: (Array[Int], Array[Int], Array[Int]),
                 wt: util.HashMap[Integer, Array[Integer]],
                 nTopics: util.HashMap[Integer, Integer],
                 p: Array[Double],
                 alpha: Double,
                 beta: Double,
                 V: Int) : (Array[Int], Array[Int], Array[Int]) = {

      val (ids, topics, docTopics) = doc

      for (i <- 0 to ids.length - 1) {

        val topic = topics(i);
        val wordID = ids(i)
        val wTopics = wt.get(wordID);

        nTopics(topic) -= 1;
        wTopics(topic) -= 1;
        docTopics(topic) -= 1;

        val Vbeta = V * beta
        val Kalpha = K * alpha

        for (k <- 0 to K - 1) {
          p(k) = (wTopics(k) + beta) / (nTopics(k) + Vbeta) *
            (docTopics(k) + alpha) / (ids.length - 1 + Kalpha)
        }

        for (k <- 1 to K - 1) {
          p(k) += p(k - 1);
        }

        val u = Math.random() * p(K - 1)
        val t = getProbability(p, 0, u)

        nTopics(t) += 1
        wTopics(t) += 1
        docTopics(t) += 1

        topics(i) = t
      }

      doc
    }

    def getProbability(p: Array[Double], index: Int, u: Double): Int = {
      if (index >= p.length)
        p.length - 1

    if (p(index) >= u)
      index

    getProbability(p, index+1, u)
  }
}

