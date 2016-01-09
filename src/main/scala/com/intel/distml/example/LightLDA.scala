package com.intel.distml.example

import java.util

import org.apache.commons.math3.distribution.UniformIntegerDistribution
import org.apache.commons.math3.distribution.UniformRealDistribution
import org.apache.spark.rdd.RDD

import _root_.scala.collection.JavaConversions._

import com.intel.distml.api.{Session, Model}
import com.intel.distml.platform.DistML
import com.intel.distml.util._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.broadcast.Broadcast
import scopt.OptionParser

import _root_.scala.collection.mutable
import _root_.scala.collection.mutable.ListBuffer

/**
 * Like AliasLDA, LightLDA uses alias tables to speed up sampling.
 *
 * Created by yunlong on 12/23/15.
 */

object LightLDA {

  val MH_STEPS = 4
/*
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
*/
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

  private def run(p: LDAParams) {
    val conf = new SparkConf().setAppName(s"DistML.Example.LDA")
    conf.set("spark.driver.maxResultSize", "5g")

    val sc = new SparkContext(conf)

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
    }).cache

    var statistics = data.map(d => (1, d._2.length)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))

    val m = new Model() {
      registerMatrix("doc-topics", new IntArrayWithIntKey(p.k))
      registerMatrix("word-topics", new IntMatrixWithIntKey(dic.getSize, p.k))
    }

    println("=============== Corpus Info Begin ================")
    println("Vocaulary: " + dic.getSize)
    println("Docs: " + statistics._1)
    println("Tokens: " + statistics._2)
    println("=============== Corpus Info End   ================")

    val dm = DistML.distribute(sc, m, p.psCount);
    val monitorPath = dm.monitorPath

    //data = data.repartition(1)

    data.mapPartitionsWithIndex(init(p, m, monitorPath)).count

    for (iter <- 0 to p.maxIterations - 1) {
      println("================= iteration: " + iter + " =====================")
      //data.mapPartitionsWithIndex(verify(p, m, monitorPath)).count()

      data = data.mapPartitionsWithIndex(train(p, m, monitorPath)).cache
      val count = data.count

      if (p.showPlexity) {
        var likeylyhood: Double = 0.0
        var n = 0
        val t1: Array[(Double, Int)] = data.mapPartitionsWithIndex(perplexity(p, m, monitorPath)).collect()
        for (item <- t1) {
          likeylyhood = likeylyhood + item._1
          n += item._2
        }

        //      val (likeylyhood, n) = data.mapPartitionsWithIndex(perplexity(p, m, monitorPath)).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
        val plexity = Math.exp(-likeylyhood / n)

        println("iteration done: " + iter + " on " + count + " samples with plexity=" + plexity)
        println("===================================")
      }
    }

    var likeylyhood : Double = 0.0
    var n = 0
    val t1 : Array[(Double, Int)] = data.mapPartitionsWithIndex(perplexity(p, m, monitorPath)).collect()
    for (item <- t1) {
      likeylyhood = likeylyhood + item._1
      n += item._2
    }

    //      val (likeylyhood, n) = data.mapPartitionsWithIndex(perplexity(p, m, monitorPath)).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    val plexity = Math.exp(-likeylyhood/n)

    println("iterations done with plexity=" + plexity)
    println("===================================")

    dm.recycle()
    sc.stop
  }

  def init(p : LDAParams, m : Model, monitorPath : String)(index : Int, it : Iterator[(Array[Int], Array[(Int, Int)])])
            : Iterator[Int] = {

    println("[" + Thread.currentThread().getId + "] Init start: ")

    val session = new Session(m, monitorPath, index)
    val dtm = m.getMatrix("doc-topics").asInstanceOf[IntArrayWithIntKey]
    val wtm = m.getMatrix("word-topics").asInstanceOf[IntMatrixWithIntKey]

    while (it.hasNext) {

      val samples = new util.LinkedList[(Array[Int], Array[(Int, Int)])]

      var count = 0
      while ((count < p.batchSize) && it.hasNext) {
        samples.add(it.next())
        count = count + 1
      }

      val keys = new KeyList()
      for ((dk, words) <- samples) {
        for (w <- words) {
          keys.addKey(w._1)
        }
      }

      val dt = new util.HashMap[Integer, Integer]
      val wt = new util.HashMap[Integer, Array[Integer]]

      for (doc <- samples) {
        val ndk = doc._1
        val words = doc._2

        for (i <- 0 to words.length - 1) {
          val wId = words(i)._1
          val topic = words(i)._2

          var wts = wt.get(wId)
          if (wts == null) {
            wts = new Array[Integer](p.k)
            for (j <- 0 to p.k - 1) {
              wts(j) = 0
            }
            wts(topic) = 1

            wt.put(wId, wts)
          } else {
            wts(topic) = wts(topic) + 1
          }

          var dtc = dt.get(topic)
          if (dtc == null) {
            dt.put(topic, new Integer(1))
          } else {
            dt.put(topic, dtc + 1)
          }
        }
      }

      //show(wt)
      //show(p, samples)

      dtm.push(dt, session)
      wtm.push(wt, session)
    }

    session.disconnect()

    println("[" + Thread.currentThread().getId + "] Init end: ")

    val r = new Array[Int](1)
    r.iterator
  }

  def train(p : LDAParams, m : Model, monitorPath : String)
           (index : Int, it : Iterator[(Array[Int], Array[(Int, Int)])]) : Iterator[(Array[Int], Array[(Int, Int)])] = {

    println("[" + Thread.currentThread().getId + "] training start: ")
    println("free memory: " + Runtime.getRuntime.freeMemory() + ", total memory: " + Runtime.getRuntime.totalMemory())
    val result = new mutable.ListBuffer[(Array[Int], Array[(Int, Int)])]

    val session = new Session(m, monitorPath, index)
    val dtm = m.getMatrix("doc-topics").asInstanceOf[IntArrayWithIntKey]
    val wtm = m.getMatrix("word-topics").asInstanceOf[IntMatrixWithIntKey]

    val samples = new util.LinkedList[(Array[Int], Array[(Int, Int)])]

    while (it.hasNext) {
      samples.clear()
      var count = 0
      while ((count < p.batchSize) && it.hasNext) {
        samples.add(it.next())
        count = count + 1
      }

      val keys = new KeyList()
      for ((dk, words) <- samples) {
        for (w <- words) {
          keys.addKey(w._1)
        }
      }

      val dt = dtm.fetch(KeyCollection.ALL, session)
      val dt_old = new util.HashMap[Int, Int]
      for (i <- 0 to p.k - 1) {
        dt_old.put(i, dt.get(i))
      }

      val wt : java.util.HashMap[Integer, Array[Integer]] = wtm.fetch(keys, session)
      val wt_old = new util.HashMap[Int, Array[Int]]
      for (key <- wt.keySet()) {
        val value = wt.get(key)
        val ts = new Array[Int](value.length)
        for (i <- 0 to value.length - 1) ts(i) = value(i)
        wt_old.put(key, ts)
      }

      //showDT(dt)
      //show(wt)
      //show(p, samples)

      val tables = new util.HashMap[Int, AliasTable]
      val q_w_proportion_ = new Array[(Int, Float)](p.k)
      for (w <- keys.keys)
        buildAliasTable(w.toInt, p.alpha, p.beta, p.alpha_sum, p.beta_sum, p.k, p.V, wt, dt, q_w_proportion_, tables)

      val u01 = new UniformRealDistribution
      val u0k = new UniformIntegerDistribution(0, p.k-1)
      val r = new java.util.Random()

      for (doc <- samples)
        sampleOneDoc(p, doc, wt, dt, tables, r, u01, u0k)

      //showDT(dt)
      //show(wt)
      //show(p, samples)

      for (key <- dt.keySet()) {
        dt.put(key, dt.get(key) - dt_old.get(key))
      }
      for (key <- wt.keySet()) {
        val ts = wt.get(key)
        val old_ts = wt_old.get(key)
        for (t <- 0 to ts.length -1) {
          if (ts(t) < 0) {
            throw new IllegalStateException("invalid counter: " + key + ", " + t + ", " + ts(t))
          }
          ts(t) = ts(t) - old_ts(t)
        }
      }


      dtm.push(dt, session)
      wtm.push(wt, session)

      for (i <- 0 to samples.length-1) {
        result.append(samples(i))
      }
    }

    session.disconnect()

    println("[" + Thread.currentThread().getId + "] training end")
    result.iterator
  }

  def verify(p : LDAParams, m : Model, monitorPath : String)(index : Int, it : Iterator[(Array[Int], Array[(Int, Int)])])
      : Iterator[Int] = {

    val session = new Session(m, monitorPath, index)
    val dtm = m.getMatrix("doc-topics").asInstanceOf[IntArrayWithIntKey]
    val wtm = m.getMatrix("word-topics").asInstanceOf[IntMatrixWithIntKey]

    val dt = dtm.fetch(KeyCollection.ALL, session)
    for (i <- 0 to p.k -1 ) {
      println("dt(" + i + ") = " + dt(i))
    }
    val wt : java.util.HashMap[Integer, Array[Integer]] = wtm.fetch(KeyCollection.ALL, session)
    for (w <- 0 to p.V - 1) {
      for (i <- 0 to p.k - 1) {
        println("wt(" + w + ")(" + i + ") = " + wt(w)(i))
      }
    }

    val t_dt = new Array[Int](p.k)
    val t_wt = new Array[Array[Int]](p.V)
    for (i <- 0 to p.V -1)
      t_wt(i) = new Array[Int](p.k)

    while(it.hasNext) {
      val (ndk, words) = it.next()

      val t_ndk = new Array[Int](p.k)
      for (i <- 0 to words.length - 1) {
        val w = words(i)._1
        val t = words(i)._2
        println("word: " + i + ", " + w + ", " + t)

        t_ndk(t) += 1
        t_dt(t) += 1
        t_wt(w)(t) += 1
      }

      for (i <- 0 to p.k - 1) {
        if (ndk(i) != t_ndk(i))
          throw new IllegalStateException("verify failed, ndk(" + i + "): " + ndk(i) + ", " + t_ndk(i))
      }
    }

    for (i <- 0 to p.k -1 ) {
      if (dt(i) != t_dt(i))
        throw new IllegalStateException("verify failed, dt(" + i + "): " + dt(i) + ", " + t_dt(i))
    }

    for (w <- 0 to p.V - 1) {
      for (i <- 0 to p.k - 1) {
        if (wt(w)(i) != t_wt(w)(i))
          throw new IllegalStateException("verify failed, wt(" + w + ")(" + i + "): " + wt(i) + ", " + t_wt(i))
      }
    }

    val r = new Array[Int](1)
    r.iterator
  }


  def inference(p : LDAParams, m : Model, monitorPath : String)
               (index : Int, it : Iterator[Array[Int]]): Iterator[Array[Int]] = {

    val session = new Session(m, monitorPath, index)
    val dtm = m.getMatrix("doc-topics").asInstanceOf[IntArrayWithIntKey]
    val wtm = m.getMatrix("word-topics").asInstanceOf[IntMatrixWithIntKey]

    var log_likelihood: Double = 0
    var nwords: Int = 0

    val samples = new util.LinkedList[(Array[Int], Array[(Int, Int)])]

    val keys = new KeyList()
    while (it.hasNext) {
      val words = it.next()

      val ndk = new Array[Int](p.k)
      val wordArray = new Array[(Int, Int)](words.length)

      for (i <- 0 to words.length - 1) {
        val w = words(i)
        keys.addKey(w)

        val t = Math.floor(Math.random() * p.k).toInt
        wordArray(i) = (i, t)
        ndk(t) += 1
      }

      samples.append((ndk, wordArray))
    }

    val dt = dtm.fetch(KeyCollection.ALL, session)
    val wt : java.util.HashMap[Integer, Array[Integer]] = wtm.fetch(keys, session)

    val prob = new Array[Double](p.k)
    for (iter <- 0 to 50) {
      for (i <- 0 to samples.size() -1 ) {
        val (ndk, words) = samples(i)

        for (n <- 0 to words.length - 1) {
          val w: Int = words(n)._1
          val old: Int = words(n)._2

          for (k <- 0 to p.k-1) {
            val theta: Double = (ndk(k) + p.alpha) / (words.length + p.alpha_sum)
            val phi: Double = (wt(w)(k) + p.beta) / (dt(k) + p.beta_sum)

            prob(k) = theta * phi
          }

          for (k <- 1 to p.k - 1) {
            prob(k) += prob(k-1)
          }

          val u = Math.random() * prob(p.k - 1)
          def f(i : Int, prob_mass : Array[Double], p: Double) : Int = { if (i == prob_mass.length) prob_mass.length-1 else if (prob_mass(i) > p) i else f(i+1, prob_mass, p) }
          val topic = f(0, prob, u)

          if (topic != old) {
            ndk(old) -= 1
            ndk(topic) += 1
            words(n) = (w, topic)
          }
        }
      }
    }

    val result = new Array[Array[Int]](samples.length)
    for (i <- 0 to samples.size() -1 ) {
      result(i) = samples(i)._1
    }

    result.iterator
  }

  def perplexity(p : LDAParams, m : Model, monitorPath : String)
                (index : Int, it : Iterator[(Array[Int], Array[(Int, Int)])]) : Iterator[(Double, Int)] = {

    val result = new Array[(Double, Int)](1)
    result(0) = (0.0, 0)

    val session = new Session(m, monitorPath, index)
    val dtm = m.getMatrix("doc-topics").asInstanceOf[IntArrayWithIntKey]
    val wtm = m.getMatrix("word-topics").asInstanceOf[IntMatrixWithIntKey]

    var log_likelihood: Double = 0.0
    var nwords: Int = 0

    val samples = new util.LinkedList[(Array[Int], Array[(Int, Int)])]

    val keys = new KeyList()
    while (it.hasNext) {
      val doc = it.next()

      val words = doc._2
      for (i <- 0 to words.length - 1) {
        keys.addKey(words(i)._1)
      }

      samples.add(doc)
    }

    val nk = dtm.fetch(KeyCollection.ALL, session)
    val nwk : java.util.HashMap[Integer, Array[Integer]] = wtm.fetch(keys, session)

    for (i <- 0 to samples.length - 1) {
      val doc = samples(i)

      val ndk = doc._1
      val words = doc._2

      nwords += words.length
      for (n <- 0 to words.length - 1) {
        var l: Double = 0
        val w: Int = words(n)._1

        for (k <- 0 to p.k-1) {
          val theta: Double = (ndk(k) + p.alpha) / (words.length + p.alpha_sum)
          val phi: Double = (nwk(w)(k) + p.beta) / (nk(k) + p.beta_sum)
          l += theta * phi
        }
        log_likelihood += Math.log(l)
      }
    }

    result(0) = (log_likelihood, nwords)
    result.iterator
  }

  def buildAliasTable(w : Int, alpha : Double, beta : Double, alpha_sum : Double, beta_sum : Double,
                      K : Int, V : Int,
                      nwk: util.HashMap[Integer, Array[Integer]],
                      nk : util.HashMap[Integer, Integer],
                      q_w_proportion_ : Array[(Int, Float)], tables : util.HashMap[Int, AliasTable]): Unit = {

    var sum : Float = 0.0f
    for (k <- 0 to K - 1) {
      q_w_proportion_(k) = (k, ((nwk(w)(k) + beta)/nk(k) + beta * V).toFloat)
      sum += q_w_proportion_(k)._2
    }

    val table = new AliasTable(K)
    table.init(q_w_proportion_, sum)
    tables.put(w, table)

//    tables.put(w, AliasTable.generateAlias(q_w_proportion_.iterator, sum, K))
  }

  def showDT(nk : java.util.HashMap[Integer, Integer]) {
    println("========showing global nk===========")
    for (i <- nk.keySet()) {
      print("n[" + i + "] = " + nk(i))
    }
  }

  def show(nwk : java.util.HashMap[Integer, Array[Integer]]) {
    println("========showing global nwk===========")
    for (i <- nwk.keySet()) {
      print("w[" + i + "]")
      val ts = nwk.get(i)
      for (j <- 0 to ts.length -1) {
        print(" " + nwk(i)(j))
      }
      println
    }
  }

  def show(p : LDAParams, samples : util.LinkedList[(Array[Int], Array[(Int, Int)])]): Unit = {
    println("========showing local nwk===========")

    val nwk = new Array[Array[Int]](p.V)
    for (i <- 0 to p.V -1 ) {
      nwk(i) = new Array[Int](p.k)
    }

    for (i <- 0 to samples.length - 1) {
      val doc = samples(i)

      val words = doc._2

      for (n <- 0 to words.length - 1) {
        val w: Int = words(n)._1
        val t: Int = words(n)._2
        nwk(w)(t) += 1
      }
    }

    for (i <- 0 to p.V -1 ) {
      print("w[" + i + "]")
      for (j <- 0 to p.k -1) {
        print(" " + nwk(i)(j))
      }
      println
    }
  }

  def sampleOneDoc(p : LDAParams, doc : (Array[Int], Array[(Int, Int)]),
                   nwk: util.HashMap[Integer, Array[Integer]],
                   nk : util.HashMap[Integer, Integer],
                   tables : util.HashMap[Int, AliasTable],
                   r : java.util.Random,
                   u01: UniformRealDistribution,
                   u0k: UniformIntegerDistribution): Unit = {

    val ndk = doc._1
    val words = doc._2

    for (i <- 0 to words.length -1 ) {

      val (w, old_topic) = words(i)
      val aliasTable = tables(w)

      val new_topic = sample(p.alpha, p.beta, p.alpha_sum, p.beta_sum, p.k,
        nwk, nk, ndk, words, i, tables(w), r, u01, u0k)

      if (old_topic != new_topic) {
        words(i) = (w, new_topic)

        ndk(old_topic) -= 1
        nwk(w)(old_topic) -= 1
        nk(old_topic) -= 1

//        if ((ndk(old_topic) < 0) || (nwk(w)(old_topic) < 0) || (nk(old_topic) < 0))
//          throw new IllegalStateException("invalid word-topic counter: "
//            + ndk(old_topic) + ", " + nwk(w)(old_topic) + ", " + nk(old_topic)
//            + ", " + w + ", " + old_topic)

        if (nwk(w)(old_topic) < 0)
          throw new IllegalStateException("invalid word-topic counter: "
            + nwk(w)(old_topic) + ", " + w + ", " + old_topic)

        ndk(new_topic) += 1
        nwk(w)(new_topic) += 1
        nk(new_topic) += 1

//        println("updated: " + w + ", " + old_topic + ", " + new_topic + ", " + nwk(w)(old_topic) + ", " + nwk(w)(new_topic))
      }
    }
  }

  def sample(alpha : Double, beta : Double, alpha_sum : Double, beta_sum : Double, K : Int,
             nwk: util.HashMap[Integer, Array[Integer]],
             nk : util.HashMap[Integer, Integer],
             ndk : Array[Int], words : Array[(Int, Int)],
             index : Int,
             alias : AliasTable,
             r : java.util.Random,
             u01: UniformRealDistribution,
             u0k: UniformIntegerDistribution) : Int = {

    val word = words(index)
    val w = word._1
    val old_topic = word._2

    var s = old_topic

    for (i <- 1 to MH_STEPS) {

      // Word proposal
      var t = alias.sampleAlias(r)

      if (t != s) {
        val rejection = u01.sample()

        val w_t_cnt = nwk(w)(t)
        val w_s_cnt = nwk(w)(s)
        val n_t = nk(t)
        val n_s = nk(s)

        var n_td_alpha = ndk(t) + alpha
        var n_sd_alpha = ndk(s) + alpha
        var n_tw_beta = w_t_cnt + beta
        var n_t_beta_sum = n_t + beta_sum
        var n_sw_beta = w_s_cnt + beta
        var n_s_beta_sum = n_s + beta_sum

        if (s == old_topic) {
          n_sd_alpha -= 1
          n_sw_beta -= 1
          n_s_beta_sum -= 1
        }
        if (t == old_topic) {
          n_td_alpha -= 1
          n_tw_beta -= 1
          n_t_beta_sum -= 1
        }

        val proposal_s = (w_s_cnt + beta) / (n_s + beta_sum)
        val proposal_t = (w_t_cnt + beta) / (n_t + beta_sum)

        val nominator = n_td_alpha * n_tw_beta * n_s_beta_sum * proposal_s
        val denominator = n_sd_alpha * n_sw_beta * n_t_beta_sum * proposal_t

        val pi = nominator / denominator

        if (rejection < pi)
          s = t
      }

      // Doc proposal
      val n_td_or_alpha = u01.sample() * (words.length + alpha * K)
      if (n_td_or_alpha < words.length) {
        val t_idx = n_td_or_alpha.toInt
        t = words(t_idx)._2
      }
      else {
        t = u0k.sample()
      }

      if (t != s) {
        val rejection = u01.sample()

        val w_t_cnt = nwk(w)(t)
        val w_s_cnt = nwk(w)(s)
        val n_t = nk(t)
        val n_s = nk(s)

        var n_td_alpha = ndk(t) + alpha
        var n_sd_alpha = ndk(s) + alpha
        var n_tw_beta = w_t_cnt + beta
        var n_t_beta_sum = n_t + beta_sum
        var n_sw_beta = w_s_cnt + beta
        var n_s_beta_sum = n_s + beta_sum
        if (s == old_topic) {
          n_sd_alpha -= 1
          n_sw_beta -= 1
          n_s_beta_sum -= 1
        }
        if (t == old_topic) {
          n_td_alpha -= 1
          n_tw_beta -= 1
          n_t_beta_sum -= 1
        }

        val proposal_s = ndk(s) + alpha
        val proposal_t = ndk(t) + alpha

        val nominator = n_td_alpha * n_tw_beta * n_s_beta_sum * proposal_s
        val denominator = n_sd_alpha * n_sw_beta * n_t_beta_sum * proposal_t

        val pi = nominator / denominator;

        if (rejection < pi)
          s = t
      }
    }

    s
  }
}