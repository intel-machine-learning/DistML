package com.intel.distml.feature

import java.io.DataOutputStream
import java.net.URI
import java.util
import java.util.Properties

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import com.intel.distml.api.{Model, Session}
import com.intel.distml.platform.{DistML, Clock}
import com.intel.distml.util.store.{DoubleArrayStore, FloatMatrixStoreAdaGrad}
import com.intel.distml.util.{DataStore, KeyList}
import com.intel.distml.util.scala.{DoubleArrayWithIntKey, FloatMatrixAdapGradWithIntKey}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Random

/**
 * Created by yunlong on 16-3-23.
 */
private[feature] class HuffmanNode extends Serializable {

  var name : String = ""

  var index = 0
  var binary = 0
  var frequency = 0L
  var parentIndex = 0

  var codeLen = 0
  var code = new Array[Int](Word2Vec.MAX_CODE_LENGTH)
  var point = new Array[Int](Word2Vec.MAX_CODE_LENGTH)

  override def clone() : HuffmanNode = {
    val w = new HuffmanNode()
    w.index = index
    w.codeLen = codeLen
    for (i <- 0 to Word2Vec.MAX_CODE_LENGTH - 1) {
      w.code(i) = code(i)
      w.point(i) = point(i)
    }

    w
  }
}

private[feature]class WordNode extends Serializable {
  //    var name : String = null
  var index = 0
  var codeLen = 0
  var code: Array[Int] = null
  var point: Array[Int] = null

  def initFrom( w : HuffmanNode) {
    //      name = w.name
    index = w.index
    codeLen = w.codeLen
    if (codeLen > 0) {
      code = new Array[Int](codeLen)
      point = new Array[Int](codeLen)
      for (i <- 0 to codeLen - 1) {
        code(i) = w.code(i)
      }
      for (i <- 0 to codeLen - 1) {
        point(i) = w.point(i)
      }
    }
  }

  override def clone() : WordNode = {
    val w = new WordNode()
    //      w.name = name
    w.index = index
    w.codeLen = codeLen
    if (codeLen > 0) {
      w.code = new Array[Int](codeLen)
      w.point = new Array[Int](codeLen)
      for (i <- 0 to codeLen - 1) {
        w.code(i) = code(i)
      }
      for (i <- 0 to codeLen - 1) {
        w.point(i) = point(i)
      }
    }
    w
  }
}

private[feature] class WordTree(
                val vocabSize : Int,
                val words : Array[WordNode]
                ) extends Serializable{

  //    var initialAlpha : Float = 0f
  //    var alpha : Float = 0f
  var tokens : Long = 0

  def getWord(index : Int) : WordNode = {
    return words(index)
  }

  def nodeCount() : Int = {
    return words.length
  }

  def show(): Unit = {
    for (w <- words) {
      println("word: " + w.index)
    }
  }
}

class Word2VecModel(
val vocabSize : Int,
val vectorSize : Int) extends Model {

  registerMatrix("syn0", new FloatMatrixAdapGradWithIntKey(vocabSize, vectorSize))
  registerMatrix("syn1", new FloatMatrixAdapGradWithIntKey(vocabSize, vectorSize))

}

object Word2Vec {

  final val MAX_CODE_LENGTH = 50

  final val EXP_TABLE_SIZE: Int = 1000
  final val MAX_EXP: Int = 6

  def load(sc: SparkContext, hdfsPath: String): DistML[Iterator[(Int, String, DataStore)]] = {

    // read meta
    val props = DistML.loadMeta(hdfsPath)

    val vocabSize = Integer.parseInt(props.get("vocabSize").asInstanceOf[String])
    val vectorSize = Integer.parseInt(props.get("vectorSize").asInstanceOf[String])
    val psCount = Integer.parseInt(props.get("psCount").asInstanceOf[String])

    // create distributed model and load parameters
    val m = new Word2VecModel(vocabSize, vectorSize)


    val dm = DistML.distribute(sc, m, psCount, DistML.defaultF)
    val monitorPath = dm.monitorPath

    dm.load(hdfsPath)

    dm
  }

  def save(dm: DistML[Iterator[(Int, String, DataStore)]], hdfsPath: String, comments: String) {
    dm.save(hdfsPath)

    // save meta
    val m = dm.model.asInstanceOf[Word2VecModel]
    val w = m.getMatrix("syn0").asInstanceOf[DoubleArrayWithIntKey]

    val props = new Properties()
    props.put("vocabSize", "" + m.vocabSize)
    props.put("vectorSize", "" + m.vectorSize)
    props.put("psCount", "" + dm.psCount)

    DistML.saveMeta(hdfsPath, props, comments)
  }

  def train(sc : SparkContext, psCount : Int, data : RDD[Array[Int]], vectorSize : Int, wordTree : WordTree, initialAlpha : Float, alphaFactor : Double,
            useCbow : Boolean, maxIterations : Int, window : Int, batchSize : Int): DistML[Iterator[(Int, String, DataStore)]] = {

    val btree = sc.broadcast(wordTree)

    val m = new Word2VecModel(wordTree.vocabSize, vectorSize)

    val dm = DistML.distribute(sc, m, psCount, DistML.defaultF)

    dm.random("syn0")
    dm.zero("syn1")
    dm.setAlpha("syn0", initialAlpha, initialAlpha * 0.0001f, alphaFactor.toFloat)
    dm.setAlpha("syn1", initialAlpha, initialAlpha * 0.0001f, alphaFactor.toFloat)

    var alpha = initialAlpha

    trainASGD(data, dm, vectorSize, btree, useCbow, maxIterations, window, batchSize)
    //trainSSP(data, dm, vectorSize, btree, useCbow, maxIterations, window)

    dm
  }

  def trainASGD(data : RDD[Array[Int]], dm : DistML[Iterator[(Int, String, DataStore)]], vectorSize : Int, btree : Broadcast[WordTree],
            useCbow : Boolean, maxIterations : Int, window : Int, batchSize : Int): Unit = {
    val m = dm.model
    val monitorPath = dm.monitorPath

    dm.setTrainSetSize(data.count())
    for (iter <- 0 to maxIterations - 1) {
      println("============ Iteration Begin: " + iter + " ==============")

      val t = data.mapPartitionsWithIndex((index, it) => {

        println("--- connecting to PS ---")
        val session = new Session(m, monitorPath, index)
        val syn0d = m.getMatrix("syn0").asInstanceOf[FloatMatrixAdapGradWithIntKey]
        val syn1d = m.getMatrix("syn1").asInstanceOf[FloatMatrixAdapGradWithIntKey]

        val wordTree = btree.value
        var trainedWords = 0L
        val syn0s = new Array[Array[Float]](wordTree.vocabSize)
        val syn1s = new Array[Array[Float]](wordTree.vocabSize)
        val alpha0s = new Array[Array[Float]](wordTree.vocabSize)
        val alpha1s = new Array[Array[Float]](wordTree.vocabSize)

        var nextRandom: Long = 5
        var expTable: Array[Float] = null;
        var neu1 = new Array[Float](vectorSize)
        var neu1e = new Array[Float](vectorSize)

        expTable = new Array[Float](EXP_TABLE_SIZE)
        var i: Int = 0
        for (i <- 0 to EXP_TABLE_SIZE-1) {
          expTable(i) = math.exp(((i / EXP_TABLE_SIZE.asInstanceOf[Float] * 2 - 1) * MAX_EXP)).toFloat
          expTable(i) = expTable(i) / (expTable(i) + 1)
        }

        val fetchClock = new Clock("Fetch")
        val trainClock = new Clock("Train")
        val pushClock = new Clock("Push")

        var wc = 0L
        var lastWc = 0L
        val samples = new mutable.MutableList[Array[Int]]
        while (it.hasNext) {
          samples.clear()
          var count = 0
          var wordCount = 0
          while ((count < batchSize) && it.hasNext) {
            val line = it.next()
            samples += line
            count += 1
            wordCount += line.length
          }

          val keys = new KeyList()
          for(wIds <- samples) {
            for (wId <- wIds) {
              keys.addKey(wId)
              val w = wordTree.getWord(wId)
              for (d <- 0 to w.codeLen-1) {
                keys.addKey(w.point(d))
              }
            }
          }

          fetchClock.start()
          val s0s = syn0d.fetch(keys, session)
          val s1s = syn1d.fetch(keys, session)
          for (key <- keys.keys) {
            val index = key.toInt
            val s0 = s0s.get(index).get._1
            val s1 = s1s.get(index).get._1
            val a0 = s0s.get(index).get._2
            val a1 = s1s.get(index).get._2
            syn0s(index) = s0
            syn1s(index) = s1
            alpha0s(index) = a0
            alpha1s(index) = a1
          }
          fetchClock.stop()

          val syn0s_old = syn0d.cloneData(s0s)
          val syn1s_old = syn1d.cloneData(s1s)

          trainClock.start()
          for (wIds : Array[Int] <- samples) {
            val sentence = new Array[WordNode](wIds.length)
            for (i <- 0 to wIds.length - 1) {
              sentence(i) = wordTree.getWord(wIds(i))
            }

            for (sentence_pos <- 0 to sentence.size - 1) {
              nextRandom = nextRandom * 25214903917L + 11
              var b = (nextRandom % window).toInt
              if (useCbow) {
                cbow(wordTree, sentence, sentence_pos, window, b, syn0s, syn1s, alpha0s, alpha1s, neu1, neu1e, expTable)
              }
              else {
                skipGram(wordTree, sentence, sentence_pos, window, b, syn0s, syn1s, alpha0s, alpha1s, neu1, neu1e, expTable)
              }
            }
          }
          trainClock.stop()

          pushClock.start()
          syn0d.getUpdate(s0s, syn0s_old)
          syn0d.push(syn0s_old, session)
          syn1d.getUpdate(s1s, syn1s_old)
          syn1d.push(syn1s_old, session)
          for (key <- keys.keys) {
            val k = key.toInt
            syn0s(k) = null
            syn1s(k) = null
            alpha0s(k) = null
            alpha1s(k) = null
          }
          pushClock.stop()

          session.progress(samples.size)
        }

        println("--- disconnect ---")
        session.disconnect()

        val r = new Array[Double](1)
        r(0) = 1
        r.iterator
      })

      t.count()

      dm.iterationDone()
      println("============ Iteration End: " + iter + " ==============")
    }
  }

  def trainSSP(data : RDD[Array[Int]], dm : DistML[Iterator[(Int, String, DataStore)]], vectorSize : Int, btree : Broadcast[WordTree],
            useCbow : Boolean, maxIterations : Int, window : Int): Unit = {
    val m = dm.model
    val monitorPath = dm.monitorPath

    dm.setTrainSetSize(data.count())
    dm.startSSP(maxIterations, 2)

    val t = data.mapPartitionsWithIndex((index, it) => {

        println("--- connecting to PS ---")
        val session = new Session(m, monitorPath, index)
        val syn0d = m.getMatrix("syn0").asInstanceOf[FloatMatrixAdapGradWithIntKey]
        val syn1d = m.getMatrix("syn1").asInstanceOf[FloatMatrixAdapGradWithIntKey]

        val wordTree = btree.value
        var trainedWords = 0L
        val syn0s = new Array[Array[Float]](wordTree.vocabSize)
        val syn1s = new Array[Array[Float]](wordTree.vocabSize)
        val alpha0s = new Array[Array[Float]](wordTree.vocabSize)
        val alpha1s = new Array[Array[Float]](wordTree.vocabSize)

        var nextRandom: Long = 5
        var expTable: Array[Float] = null;
        var neu1 = new Array[Float](vectorSize)
        var neu1e = new Array[Float](vectorSize)

        expTable = new Array[Float](EXP_TABLE_SIZE)
        var i: Int = 0
        for (i <- 0 to EXP_TABLE_SIZE-1) {
          expTable(i) = math.exp(((i / EXP_TABLE_SIZE.asInstanceOf[Float] * 2 - 1) * MAX_EXP)).toFloat
          expTable(i) = expTable(i) / (expTable(i) + 1)
        }

        val fetchClock = new Clock("Fetch")
        val trainClock = new Clock("Train")
        val pushClock = new Clock("Push")

        var wc = 0L
        var lastWc = 0L
        val samples = new mutable.MutableList[Array[Int]]
        while (it.hasNext) {
          samples.clear()
          while (it.hasNext) {
            val line = it.next()
            samples += line
          }
        }

          val keys = new KeyList()
          for(wIds <- samples) {
            for (wId <- wIds) {
              keys.addKey(wId)
              val w = wordTree.getWord(wId)
              for (d <- 0 to w.codeLen-1) {
                keys.addKey(w.point(d))
              }
            }
          }

        for (iter <- 0 to maxIterations - 1) {
          println("============ Iteration Begin: " + iter + " ==============")

          fetchClock.start()
          val s0s = syn0d.fetch(keys, session)
          val s1s = syn1d.fetch(keys, session)
          for (key <- keys.keys) {
            val index = key.toInt
            val s0 = s0s.get(index).get._1
            val s1 = s1s.get(index).get._1
            val a0 = s0s.get(index).get._2
            val a1 = s1s.get(index).get._2
            syn0s(index) = s0
            syn1s(index) = s1
            alpha0s(index) = a0
            alpha1s(index) = a1
          }
          fetchClock.stop()

          val syn0s_old = syn0d.cloneData(s0s)
          val syn1s_old = syn1d.cloneData(s1s)

          trainClock.start()
          var count = 0;
          for (wIds : Array[Int] <- samples) {
            val sentence = new Array[WordNode](wIds.length)
            for (i <- 0 to wIds.length - 1) {
              sentence(i) = wordTree.getWord(wIds(i))
            }

            for (sentence_pos <- 0 to sentence.size - 1) {
              nextRandom = nextRandom * 25214903917L + 11
              var b = (nextRandom % window).toInt
              if (useCbow) {
                cbow(wordTree, sentence, sentence_pos, window, b, syn0s, syn1s, alpha0s, alpha1s, neu1, neu1e, expTable)
              }
              else {
                skipGram(wordTree, sentence, sentence_pos, window, b, syn0s, syn1s, alpha0s, alpha1s, neu1, neu1e, expTable)
              }
            }

            count += 1
            if (count % 1000 == 0) {
              println("progress: " + count)
            }
          }
          trainClock.stop()

          pushClock.start()
          syn0d.getUpdate(s0s, syn0s_old)
          syn0d.push(syn0s_old, session)
          syn1d.getUpdate(s1s, syn1s_old)
          syn1d.push(syn1s_old, session)
          for (key <- keys.keys) {
            val k = key.toInt
            syn0s(k) = null
            syn1s(k) = null
            alpha0s(k) = null
            alpha1s(k) = null
          }
          pushClock.stop()

          session.iterationDone(iter)
        }

        println("--- disconnect ---")
        session.disconnect()

        val r = new Array[Double](1)
        r(0) = 1
        r.iterator
    })

    t.count()
  }


  def cbow(wordTree : WordTree,
           sentence: Array[WordNode],
           index: Int,
           windowSize : Int,
           b: Int,
           syn0s : Array[Array[Float]],
           syn1s : Array[Array[Float]],
           alpha0s : Array[Array[Float]],
           alpha1s : Array[Array[Float]],
           neu1 : Array[Float],
           neu1e : Array[Float],
           expTable : Array[Float]
            ) {

    val word = sentence(index)

    var a: Int = 0
    var c: Int = 0
    var tmp = 0.0f

    clear(neu1)
    clear(neu1e)
    var cw = 0
    for (a <- b to windowSize * 2 - b) {
      c = index - windowSize + a
      if ((a != windowSize) && (c >= 0 && c < sentence.size)) {

        val laswWordSyn0 = syn0s(sentence(c).index)
        add(neu1, laswWordSyn0)
        cw += 1
      }
    }

    if (cw > 0) {
      for (i <- 0 to neu1.length - 1) {
        neu1(i) /= cw
      }

      for (d <- 0 to word.codeLen - 1) {
        val l2Syn1 = syn1s(word.point(d))
        val l2Alpha1 = alpha1s(word.point(d))
        var f: Float = dotProductSyn1(neu1, l2Syn1)

        if (f > -MAX_EXP && f < MAX_EXP) {
          f = (f + MAX_EXP) * (EXP_TABLE_SIZE / MAX_EXP / 2)
          f = expTable(f.asInstanceOf[Int])
          val g = (1.0f - word.code(d) - f)
          addWitWeight(neu1e, l2Syn1, g)
          calculateDelta1(l2Syn1, l2Alpha1, neu1, g)
        }
      }

      for (a <- b to windowSize * 2 - b) {
        c = index - windowSize + a
        if ((a != windowSize) && (c >= 0 && c < sentence.size)) {
          val lSyn0 = syn0s(sentence(c).index)
          val lalpha0 = alpha0s(sentence(c).index)
          calculateDelta0(lSyn0, lalpha0, neu1e)
        }
      }
    }
  }

  def skipGram(wordTree : WordTree,
               sentence: Array[WordNode],
               index: Int,
               windowSize : Int,
               b: Int,
               syn0s : Array[Array[Float]],
               syn1s : Array[Array[Float]],
               alpha0s : Array[Array[Float]],
               alpha1s : Array[Array[Float]],
               neu1 : Array[Float],
               neu1e : Array[Float],
               expTable : Array[Float]
                ) {

    val word = sentence(index)
    var a: Int = 0
    var c: Int = 0

    for (a <- b to windowSize * 2 - b) {
      c = index - windowSize + a
      if ((a != windowSize) && (c >= 0 && c < sentence.size)) {

        val lSyn0 = syn0s(sentence(c).index)
        val lAlpha0 = alpha0s(sentence(c).index)

        for (i <- 0 to neu1e.length-1) {
          neu1e(i) = 0.0f
        }

        for (d <- 0 to word.codeLen-1) {
          val outSyn1 = syn1s(word.point(d))
          val outAlpha1 = alpha1s(word.point(d))

          var f = blas.sdot(lSyn0.length, lSyn0, 0, 1, outSyn1, 0, 1)
          if (f > -MAX_EXP && f < MAX_EXP) {
            f = (f + MAX_EXP) * (EXP_TABLE_SIZE / MAX_EXP / 2)
            f = expTable(f.asInstanceOf[Int])
            //AdapGrad
            var g =  (1.0f - word.code(d) - f)
            addWitWeight(neu1e, outSyn1, g)
            calculateDelta1(outSyn1, outAlpha1, lSyn0, g)
          }
        }

        calculateDelta0(lSyn0, lAlpha0, neu1e)
      }
    }
  }

  def skipGram2(wordTree : WordTree,
                sentence: mutable.MutableList[WordNode],
                index: Int,
                windowSize : Int,
                b: Int,
                alpha : Float,
                syn0s : mutable.HashMap[Int, Array[Float]],
                syn1s : mutable.HashMap[Int, Array[Float]],
                neu1 : Array[Float],
                neu1e : Array[Float],
                expTable : Array[Float],
                mapClock : Clock,
                computeClock : Clock
                 ) {

    val word = sentence(index)
    var a: Int = 0
    var c: Int = 0

    for (a <- b to windowSize * 2 - b) {
      c = index - windowSize + a
      if ((a != windowSize) && (c >= 0 && c < sentence.size)) {

        mapClock.resume()
        val lSyn0 = syn0s.get(sentence(c).index).get
        mapClock.pause()

        computeClock.resume()
        for (i <- 0 to neu1e.length-1) {
          neu1e(i) = 0.0f
        }
        computeClock.pause()

        for (d <- 0 to word.codeLen-1) {
          mapClock.resume()
          val outSyn1 = syn1s.get(word.point(d)).get
          mapClock.pause()

          computeClock.resume()
          var f = blas.sdot(lSyn0.length, lSyn0, 0, 1, outSyn1, 0, 1)
          //var f: Float = dotProductSyn1(lSyn0, outSyn1)
          if (f > -MAX_EXP && f < MAX_EXP) {
            f = (f + MAX_EXP) * (EXP_TABLE_SIZE / MAX_EXP / 2)
            f = expTable(f.asInstanceOf[Int])
            var g =  (1.0f - word.code(d) - f) * alpha
            blas.saxpy(neu1e.length, g, outSyn1, 0, 1, neu1e, 0, 1)
            blas.saxpy(neu1.length, g, lSyn0, 0, 1, outSyn1, 0, 1)
            //addWitWeight(neu1e, outSyn1, g)
            //addWitWeight(outSyn1, lSyn0, g)
          }
          computeClock.pause()
        }

        computeClock.resume()
        blas.saxpy(neu1e.length, 1, neu1e, 0, 1, lSyn0, 0, 1)
        //add(lSyn0, neu1e)
        computeClock.pause()
      }
    }
  }

  def createBinaryTree(alphabet : Array[(String, Long)]) : WordTree = {

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

    return new WordTree(vocabSize, t)
  }

  private def clear(data : Array[Float]) {
    for (i <- 0 to data.length - 1) {
      data(i) = 0.0f
    }
  }

  private def add(data : Array[Float], d : Array[Float]) {
    for (c <- 0 to data.length - 1) {
      data(c) += d(c)
    }
  }

  private def addWitWeight(data : Array[Float], d : Array[Float], g : Float) {
    for (c <- 0 to data.length - 1) {
      data(c) += g * d(c)
    }
  }

  private def dotProductSyn1(data : Array[Float], d : Array[Float]) : Float = {
    var f = 0.0f
    for (c <- 0 to data.length - 1) {
      f += data(c) * d(c)
    }
    f
  }

  private def calculateDelta1(data : Array[Float], alpha : Array[Float], d : Array[Float], g : Float) {
    for (c <- 0 to data.length - 1) {
      data(c) += g * d(c) * alpha(c)
    }
  }

  private def calculateDelta0(data : Array[Float], alpha : Array[Float], d : Array[Float]) {
    var tmp = 0.0f
    for (c <- 0 to data.length - 1) {
      data(c) += d(c) * alpha(c)
    }
  }

  def findSynonyms(words : Array[Array[Float]], w: Array[Float], k: Int): Array[(Int, Double)] = {
    words.zipWithIndex.map(word => (word._2, similarity(w, word._1)))
      .sortBy(_._2).reverse
      .take(k+1)
      .tail
      .toArray
  }

  def similarity(v1: Array[Float], v2: Array[Float]): Double = {

    val norm1 = v1.map(v => v * v).reduce((s1, s2) => s1 + s2)
    val norm2 = v2.map(v => v * v).reduce((s1, s2) => s1 + s2)

    if (norm1 < 10e-106 || norm2 < 10e-10) return 0.0f

    (v1, v2).zipped.map(_ * _).reduce( _ + _) / Math.sqrt(norm1) / Math.sqrt(norm2)
  }

  def collect(dm : DistML[Iterator[(Int, String, DataStore)]]): Array[(Int, Array[Float])] = {
    val allWeights = dm.params().flatMap(defaultF).collect()

    allWeights
  }

  def defaultF(it : Iterator[(Int, String, DataStore)])
  : Iterator[(Int, Array[Float])] = {

    println("collecting model parameters...")

    var store : FloatMatrixStoreAdaGrad = null
    while (it.hasNext) {
      val storeParts = it.next()
      if (storeParts._2.equals("syn0"))
        store = storeParts._3.asInstanceOf[FloatMatrixStoreAdaGrad]
    }

    val vectors = store.iter()

    var result = new util.LinkedList[(Int, Array[Float])]
    while (vectors.hasNext) {
      vectors.next()
      val a : Array[Float] = new Array[Float](2)
      result.add((vectors.key().toInt, vectors.value()))
    }

    println("done")
    result.iterator
  }
}
