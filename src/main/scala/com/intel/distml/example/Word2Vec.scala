package com.intel.distml.example

import java.util

import scala.collection.JavaConversions._

import com.intel.distml.api.{Session, Model}
import com.intel.distml.platform.{Clock, DistML}
import com.intel.distml.util.{DataStore, KeyList}
import com.intel.distml.util.scala.{FloatMatrixAdapGradWithIntKey, FloatMatrixWithIntKey, DoubleArrayWithIntKey}
import com.intel.distml.util.store.{FloatMatrixStore, FloatMatrixStoreAdaGrad, DoubleArrayStore}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}
import com.github.fommil.netlib.BLAS.{getInstance => blas}

import scopt.OptionParser
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

object Word2Vec {

  private case class Params(
                             psCount: Int = 1,
                             numPartitions : Int = 1,
                             input: String = null,
                             cbow: Boolean = true,
                             alpha: Double = 0.0f,
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
      println("window: " + window)
      println("batchSize: " + batchSize)
      println("vectorSize: " + vectorSize)
      println("minFreq: " + minFreq)
      println("maxIterations: " + maxIterations)
      println("=========== params =============")
    }
  }

  val MAX_CODE_LENGTH = 50
  //val MIN_WORD_FREQ = 50

  //val windowSize = 7
  var initialAlpha = 0.0025f
//  val alphaThreshold = 0.0001f

  final val EXP_TABLE_SIZE: Int = 1000
  final val MAX_EXP: Int = 6

  private class HuffmanNode extends Serializable {

    var name : String = ""

    var index = 0
    var binary = 0
    var frequency = 0L
    var parentIndex = 0

    var codeLen = 0
    var code = new Array[Int](MAX_CODE_LENGTH)
    var point = new Array[Int](MAX_CODE_LENGTH)

    override def clone() : HuffmanNode = {
      val w = new HuffmanNode()
      w.index = index
      w.codeLen = codeLen
      for (i <- 0 to MAX_CODE_LENGTH - 1) {
        w.code(i) = code(i)
        w.point(i) = point(i)
      }

      w
    }
  }

  class WordNode extends Serializable {
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

  class WordTree(
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
        .text(s"learning rate. default: ${defaultParams.alpha}")
        .action((x, c) => c.copy(alpha = x))
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
    var wordTree = createBinaryTree(countedWords)
    wordTree.tokens = totalWords

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

    val btree = sc.broadcast(wordTree)
    println("vocabulary size: " + wordTree.vocabSize + ", vector size: " + p.vectorSize)

    val m = new Model() {
      registerMatrix("syn0", new FloatMatrixWithIntKey(wordTree.vocabSize, p.vectorSize))
      registerMatrix("syn1", new FloatMatrixWithIntKey(wordTree.vocabSize, p.vectorSize))
    }

    val dm = DistML.distribute(sc, m, p.psCount, defaultF)
    val monitorPath = dm.monitorPath

    dm.random("syn0")
    dm.zero("syn1")

    var alpha = initialAlpha
    for (iter <- 0 to p.maxIterations - 1) {
      println("============ Iteration Begin: " + iter + " ==============")

      val t = data.mapPartitionsWithIndex((index, it) => {

        System.loadLibrary("floatops")

        println("--- connecting to PS ---")
        val session = new Session(m, monitorPath, index)
        val syn0d = m.getMatrix("syn0").asInstanceOf[FloatMatrixWithIntKey]
        val syn1d = m.getMatrix("syn1").asInstanceOf[FloatMatrixWithIntKey]

        val wordTree = btree.value
        var trainedWords = 0L
        val syn0s = new Array[Array[Float]](wordTree.vocabSize)
        val syn1s = new Array[Array[Float]](wordTree.vocabSize)

        var nextRandom: Long = 5
        var expTable: Array[Float] = null;
        var neu1 = new Array[Float](p.vectorSize)
        var neu1e = new Array[Float](p.vectorSize)

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
          while ((count < p.batchSize) && it.hasNext) {
            val line = it.next()
            samples += line
            count += 1
            wordCount += line.length
          }

          if (wc - lastWc > 10000) {
            lastWc = wc
            alpha = initialAlpha * (1 - p.numPartitions * wc) / (wordTree.tokens + 1)
            if (alpha < initialAlpha * 0.0001f)
              alpha =  initialAlpha * 0.0001f
          }
          wc += wordCount

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
            syn0s(key.toInt) = s0s.get(key.toInt).get
            syn1s(key.toInt) = s1s.get(key.toInt).get
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
              var b = (nextRandom % p.window).toInt
              if (p.cbow) {
                cbow(wordTree, sentence, sentence_pos, p.window, b, alpha, syn0s, syn1s, neu1, neu1e, expTable)
              }
              else {
                skipGram(wordTree, sentence, sentence_pos, p.window, b, alpha, syn0s, syn1s, neu1, neu1e, expTable)
              }
            }
          }
          trainClock.stop()


          pushClock.start()
          syn0d.getUpdate(s0s, syn0s_old)
          syn0d.push(syn0s_old, session)
          syn1d.getUpdate(s1s, syn1s_old)
          syn1d.push(syn1s_old, session)
          pushClock.stop()

          session.progress(wordCount)
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

    dm.recycle()
    val result = dm.params().flatMap(it => it).collect()
    val vectors = new Array[Array[Float]](result.size)
    for (w <- result) {
      vectors(w._1) = w._2
    }

    val blackIndex = wordMap.getID("black")
    val sims = findSynonyms(vectors, vectors(blackIndex), 10)
    for (s <- sims) {
      println(wordMap.getWord(s._1) + ", " + s._2)
    }


      sc.stop()

    System.out.println("===== Finished ====")
  }


  def cbow(wordTree : WordTree,
           sentence: Array[WordNode],
           index: Int,
           windowSize : Int,
           b: Int,
           alpha : Float,
           syn0s : Array[Array[Float]],
           syn1s : Array[Array[Float]],
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
        var f: Float = dotProductSyn1(neu1, l2Syn1)

        if (f > -MAX_EXP && f < MAX_EXP) {
          f = (f + MAX_EXP) * (EXP_TABLE_SIZE / MAX_EXP / 2)
          f = expTable(f.asInstanceOf[Int])
          val g = (1.0f - word.code(d) - f) * alpha
          addWitWeight(neu1e, l2Syn1, g)
          addWitWeight(l2Syn1, neu1, g)
        }
      }

      for (a <- b to windowSize * 2 - b) {
        c = index - windowSize + a
        if ((a != windowSize) && (c >= 0 && c < sentence.size)) {
          val lSyn0 = syn0s(sentence(c).index)
          add(lSyn0, neu1e)
        }
      }
    }
  }

  def skipGram(wordTree : WordTree,
               sentence: Array[WordNode],
               index: Int,
               windowSize : Int,
               b: Int,
               alpha : Float,
               syn0s : Array[Array[Float]],
               syn1s : Array[Array[Float]],
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

        for (i <- 0 to neu1e.length-1) {
          neu1e(i) = 0.0f
        }

        for (d <- 0 to word.codeLen-1) {
          val outSyn1 = syn1s(word.point(d))

          var f = blas.sdot(lSyn0.length, lSyn0, 0, 1, outSyn1, 0, 1)
          if (f > -MAX_EXP && f < MAX_EXP) {
            f = (f + MAX_EXP) * (EXP_TABLE_SIZE / MAX_EXP / 2)
            f = expTable(f.asInstanceOf[Int])
            var g =  (1.0f - word.code(d) - f) * alpha
            blas.saxpy(neu1e.length, g, outSyn1, 0, 1, neu1e, 0, 1)
            blas.saxpy(neu1.length, g, lSyn0, 0, 1, outSyn1, 0, 1)
          }
        }

        blas.saxpy(neu1e.length, 1, neu1e, 0, 1, lSyn0, 0, 1)
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

  def clear(data : Array[Float]) {
    for (i <- 0 to data.length - 1) {
      data(i) = 0.0f
    }
  }

  def add(data : Array[Float], d : Array[Float]) {
    for (c <- 0 to data.length - 1) {
      data(c) += d(c)
    }
  }

  def addWitWeight(data : Array[Float], d : Array[Float], g : Float) {
    for (c <- 0 to data.length - 1) {
      data(c) += g * d(c)
    }
  }

  def dotProductSyn1(data : Array[Float], d : Array[Float]) : Float = {
    var f = 0.0f
    for (c <- 0 to data.length - 1) {
      f += data(c) * d(c)
    }
    f
  }

  def calculateDelta1(data : Array[Float], alpha : Array[Float], d : Array[Float], g : Float) {
    for (c <- 0 to data.length - 1) {
      data(c) += g * d(c) * alpha(c)
    }
  }

  def calculateDelta0(data : Array[Float], alpha : Array[Float], d : Array[Float]) {
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

  def defaultF(model : Model, index : Int, stores : java.util.HashMap[String, DataStore])
  : Iterator[(Int, Array[Float])] = {

    println("collecting model parameters...")
    val store = stores.get("syn0").asInstanceOf[FloatMatrixStore]
    val vectors = store.iter()

    var result = new util.LinkedList[(Int, Array[Float])]
    while (vectors.hasNext) {
      vectors.next()
      val a : Array[Float] = new Array[Float](2)
      if (vectors.key() == 100L)
        println("syn0(100)(0) = " + vectors.value()(0))
      result.add((vectors.key().toInt, vectors.value()))
    }

    println("done")
    result.iterator
  }

}
