package com.intel.distml.clustering

import java.util

import com.intel.distml.Dict
import com.intel.distml.api.{Model, Session}
import com.intel.distml.platform.DistML
import com.intel.distml.util._
import org.apache.commons.math3.distribution.{UniformIntegerDistribution, UniformRealDistribution}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

import _root_.scala.collection.JavaConversions._
import _root_.scala.collection.mutable
import _root_.scala.collection.mutable.ListBuffer

/**
 * Like AliasLDA, LightLDA uses alias tables to speed up sampling.
 *
 * Created by yunlong on 12/23/15.
 */

object LightLDA {

  val MH_STEPS = 4

  def train(sc : SparkContext, samples : RDD[(Array[Int], Array[(Int, Int)])], V: Int, p : LDAParams): DistML[Iterator[(Int, String, DataStore)]]  = {

    val m = new LDAModel(V, p.k, p.alpha, p.beta)
    val dm = DistML.distribute(sc, m, p.psCount, DistML.defaultF)
    val monitorPath = dm.monitorPath
    val batchSize = p.batchSize

    var data = samples
    val trainDataSize = samples.count
    println("dataset size: " + trainDataSize)
    dm.setTrainSetSize(trainDataSize)

    data.mapPartitionsWithIndex(init(m, monitorPath, batchSize)).persist(StorageLevel.MEMORY_AND_DISK).count()
    for (iter <- 0 to p.maxIterations - 1) {
      println("================= iteration: " + iter + " =====================")
      //data.mapPartitionsWithIndex(verify(p, m, monitorPath)).count()

      data = data.mapPartitionsWithIndex(train(m, monitorPath, batchSize)).persist(StorageLevel.MEMORY_AND_DISK)
      val count = data.count

      if (p.showPlexity) {
        var likeylyhood: Double = 0.0
        var n = 0
        val t1: Array[(Double, Int)] = data.mapPartitionsWithIndex(perplexity(m, monitorPath)).collect()
        for (item <- t1) {
          likeylyhood = likeylyhood + item._1
          n += item._2
        }

        //      val (likeylyhood, n) = data.mapPartitionsWithIndex(perplexity(p, m, monitorPath)).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
        val plexity = Math.exp(-likeylyhood / n)

        println("iteration done: " + iter + " on " + count + " samples with plexity=" + plexity)
        println("===================================")
      }

      dm.iterationDone()
    }

    dm
  }

  def init(m : LDAModel, monitorPath : String, batchSize : Int)(index : Int, it : Iterator[(Array[Int], Array[(Int, Int)])])
            : Iterator[Int] = {

    println("[" + Thread.currentThread().getId + "] Init start: ")

    val session = new Session(m, monitorPath, index)
    val dtm = m.getMatrix("doc-topics").asInstanceOf[IntArrayWithIntKey]
    val wtm = m.getMatrix("word-topics").asInstanceOf[IntMatrixWithIntKey]

    while (it.hasNext) {

      val samples = new util.LinkedList[(Array[Int], Array[(Int, Int)])]

      var count = 0
      while ((count < batchSize) && it.hasNext) {
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

        for (i <- words.indices) {
          val wId = words(i)._1
          val topic = words(i)._2

          var wts = wt.get(wId)
          if (wts == null) {
            wts = new Array[Integer](m.K)
            for (j <- 0 to m.K - 1) {
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

  def train(m : LDAModel, monitorPath : String, batchSize : Int)
           (index : Int, it : Iterator[(Array[Int], Array[(Int, Int)])]) : Iterator[(Array[Int], Array[(Int, Int)])] = {

    println("[" + Thread.currentThread().getId + "] training start: ")
    println("free memory: " + Runtime.getRuntime.freeMemory() + ", total memory: " + Runtime.getRuntime.totalMemory())
    val result = new mutable.ListBuffer[(Array[Int], Array[(Int, Int)])]

    val session = new Session(m, monitorPath, index)
    val dtm = m.getMatrix("doc-topics").asInstanceOf[IntArrayWithIntKey]
    val wtm = m.getMatrix("word-topics").asInstanceOf[IntMatrixWithIntKey]

    val samples = new util.LinkedList[(Array[Int], Array[(Int, Int)])]

    var start = System.currentTimeMillis()
    var end = start

    while (it.hasNext) {
      samples.clear()
      var count = 0
      while ((count < batchSize) && it.hasNext) {
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
      for (i <- 0 to m.K - 1) {
        dt_old.put(i, dt.get(i))
      }

      val wt : java.util.HashMap[Integer, Array[Integer]] = wtm.fetch(keys, session)
      val wt_old = new util.HashMap[Int, Array[Int]]
      for (key <- wt.keySet()) {
        val value = wt.get(key)
        val ts = new Array[Int](value.length)
        for (i <- value.indices) ts(i) = value(i)
        wt_old.put(key, ts)
      }

      end = System.currentTimeMillis()
      println("prefetch: " + (end-start))
      start = end
      //showDT(dt)
      //show(wt)
      //show(p, samples)

      val tables = new util.HashMap[Int, AliasTable]
      val q_w_proportion_ = new Array[(Int, Float)](m.K)
      for (w <- keys.keys)
        buildAliasTable(w.toInt, m.alpha, m.beta, m.alpha_sum, m.beta_sum, m.K, m.V, wt, dt, q_w_proportion_, tables)

      end = System.currentTimeMillis()
      println("build table: " + (end-start))
      start = end

      val u01 = new UniformRealDistribution
      val u0k = new UniformIntegerDistribution(0, m.K-1)
      val r = new java.util.Random()

      for (doc <- samples)
        sampleOneDoc(m, doc, wt, dt, tables, r, u01, u0k)

      end = System.currentTimeMillis()
      println("sample: " + (end-start))
      start = end

      //showDT(dt)
      //show(wt)
      //show(p, samples)

      for (key <- dt.keySet()) {
        dt.put(key, dt.get(key) - dt_old.get(key))
      }
      for (key <- wt.keySet()) {
        val ts = wt.get(key)
        val old_ts = wt_old.get(key)
        for (t <- ts.indices) {
          if (ts(t) < 0) {
            throw new IllegalStateException("invalid counter: " + key + ", " + t + ", " + ts(t))
          }
          ts(t) = ts(t) - old_ts(t)
        }
      }

      dtm.push(dt, session)
      wtm.push(wt, session)

      session.progress(samples.size())

      end = System.currentTimeMillis()
      println("push: " + (end-start))
      start = end

      for (i <- samples.indices) {
        result.append(samples(i))
      }
    }

    session.disconnect()

    println("[" + Thread.currentThread().getId + "] training end")
    result.iterator
  }

  def verify(m : LDAModel, monitorPath : String)(index : Int, it : Iterator[(Array[Int], Array[(Int, Int)])])
      : Iterator[Int] = {

    val session = new Session(m, monitorPath, index)
    val dtm = m.getMatrix("doc-topics").asInstanceOf[IntArrayWithIntKey]
    val wtm = m.getMatrix("word-topics").asInstanceOf[IntMatrixWithIntKey]

    val dt = dtm.fetch(KeyCollection.ALL, session)
    for (i <- 0 to m.K -1 ) {
      println("dt(" + i + ") = " + dt(i))
    }
    val wt : java.util.HashMap[Integer, Array[Integer]] = wtm.fetch(KeyCollection.ALL, session)
    for (w <- 0 to m.V - 1) {
      for (i <- 0 to m.K - 1) {
        println("wt(" + w + ")(" + i + ") = " + wt(w)(i))
      }
    }

    val t_dt = new Array[Int](m.K)
    val t_wt = new Array[Array[Int]](m.V)
    for (i <- 0 to m.V -1)
      t_wt(i) = new Array[Int](m.K)

    while(it.hasNext) {
      val (ndk, words) = it.next()

      val t_ndk = new Array[Int](m.K)
      for (i <- words.indices) {
        val w = words(i)._1
        val t = words(i)._2
        println("word: " + i + ", " + w + ", " + t)

        t_ndk(t) += 1
        t_dt(t) += 1
        t_wt(w)(t) += 1
      }

      for (i <- 0 to m.K - 1) {
        if (ndk(i) != t_ndk(i))
          throw new IllegalStateException("verify failed, ndk(" + i + "): " + ndk(i) + ", " + t_ndk(i))
      }
    }

    for (i <- 0 to m.K -1 ) {
      if (dt(i) != t_dt(i))
        throw new IllegalStateException("verify failed, dt(" + i + "): " + dt(i) + ", " + t_dt(i))
    }

    for (w <- 0 to m.V - 1) {
      for (i <- 0 to m.K - 1) {
        if (wt(w)(i) != t_wt(w)(i))
          throw new IllegalStateException("verify failed, wt(" + w + ")(" + i + "): " + wt(i) + ", " + t_wt(i))
      }
    }

    val r = new Array[Int](1)
    r.iterator
  }


  def inference(m : LDAModel, monitorPath : String)
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

      val ndk = new Array[Int](m.K)
      val wordArray = new Array[(Int, Int)](words.length)

      for (i <- words.indices) {
        val w = words(i)
        keys.addKey(w)

        val t = Math.floor(Math.random() * m.K).toInt
        wordArray(i) = (i, t)
        ndk(t) += 1
      }

      samples.append((ndk, wordArray))
    }

    val dt = dtm.fetch(KeyCollection.ALL, session)
    val wt : java.util.HashMap[Integer, Array[Integer]] = wtm.fetch(keys, session)

    val prob = new Array[Double](m.K)
    for (iter <- 0 to 50) {
      for (i <- 0 to samples.size() -1 ) {
        val (ndk, words) = samples(i)

        for (n <- words.indices) {
          val w: Int = words(n)._1
          val old: Int = words(n)._2

          for (k <- 0 to m.K-1) {
            val theta: Double = (ndk(k) + m.alpha) / (words.length + m.alpha_sum)
            val phi: Double = (wt(w)(k) + m.beta) / (dt(k) + m.beta_sum)

            prob(k) = theta * phi
          }

          for (k <- 1 to m.K - 1) {
            prob(k) += prob(k-1)
          }

          val u = Math.random() * prob(m.K - 1)
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
    for (i <- samples.indices) {
      result(i) = samples(i)._1
    }

    result.iterator
  }

  def perplexity(m : LDAModel, monitorPath : String)
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
      for (i <- words.indices) {
        keys.addKey(words(i)._1)
      }

      samples.add(doc)
    }

    val nk = dtm.fetch(KeyCollection.ALL, session)
    val nwk : java.util.HashMap[Integer, Array[Integer]] = wtm.fetch(keys, session)

    for (i <- samples.indices) {
      val doc = samples(i)

      val ndk = doc._1
      val words = doc._2

      nwords += words.length
      for (n <- words.indices) {
        var l: Double = 0
        val w: Int = words(n)._1

        for (k <- 0 to m.K-1) {
          val theta: Double = (ndk(k) + m.alpha) / (words.length + m.alpha_sum)
          val phi: Double = (nwk(w)(k) + m.beta) / (nk(k) + m.beta_sum)
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

  def show(m : LDAModel, samples : util.LinkedList[(Array[Int], Array[(Int, Int)])]): Unit = {
    println("========showing local nwk===========")

    val nwk = new Array[Array[Int]](m.V)
    for (i <- 0 to m.V -1 ) {
      nwk(i) = new Array[Int](m.K)
    }

    for (i <- 0 to samples.length - 1) {
      val doc = samples(i)

      val words = doc._2

      for (n <- words.indices) {
        val w: Int = words(n)._1
        val t: Int = words(n)._2
        nwk(w)(t) += 1
      }
    }

    for (i <- 0 to m.V -1 ) {
      print("w[" + i + "]")
      for (j <- 0 to m.K -1) {
        print(" " + nwk(i)(j))
      }
      println
    }
  }

  def sampleOneDoc(m : LDAModel, doc : (Array[Int], Array[(Int, Int)]),
                   nwk: util.HashMap[Integer, Array[Integer]],
                   nk : util.HashMap[Integer, Integer],
                   tables : util.HashMap[Int, AliasTable],
                   r : java.util.Random,
                   u01: UniformRealDistribution,
                   u0k: UniformIntegerDistribution): Unit = {

    val ndk = doc._1
    val words = doc._2

    for (i <- words.indices) {

      val (w, old_topic) = words(i)
      val aliasTable = tables(w)

      val new_topic = sample(m.alpha, m.beta, m.alpha_sum, m.beta_sum, m.K,
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
