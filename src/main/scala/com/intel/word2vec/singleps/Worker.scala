package com.intel.word2vec.singleps

import java.nio.ByteBuffer
import java.util.Date
import org.apache.spark.broadcast.Broadcast
import scala._
import scala.collection.mutable
import scala.Predef._
import com.intel.word2vec.common.FloatOps

/**
 * Created by He Yunlong on 5/28/14.
 */
class Worker(
index : Int,
expTable: Array[Float],
wordTree : Array[NeuronNode],
wordMap : mutable.HashMap[String, Int],
totalWords : Long,
lines : Iterator[String],
startingAlpha : Float,
outputFolder : String,
paramServer : String,
pushStep : Int,
fetchStep : Int
) {

  final val EXP_TABLE_SIZE: Int = 1000
  final val MAX_EXP: Int = 6

  val sample: Double = 1e-3
  var windowSize: Int = 5
  var vectorSize: Int = 200   //notes there are many hard-coded 200 and 800, change them if needed

  var nextRandom: Long = 5
  var alphaThreshold: Float = 0.0001f

  val partitionIndex = index
  var trainedWords = 0L
  var totalTrained = 0L
  var alpha = startingAlpha

  var neu1e : Array[Float] = null
  var neu1eBuf: ByteBuffer = null

  def skipGram(index: Int,
               sentence: mutable.MutableList[NeuronNode],
               b: Int) {

    val word: NeuronNode = sentence(index)
    var a: Int = 0
    var c: Int = 0
    var tmp = 0.0f

    for (a <- b to windowSize * 2 - b) {
      c = index - windowSize + a
      if ((a != windowSize) && (c >= 0 && c < sentence.size)) {

        val lastWord: NeuronNode = sentence(c)

        for (c <- 0 to vectorSize-1) {
          neu1e(c) = 0.0f
        }

        for (d <- 0 to word.codeLen-1) {
          val out: NeuronNode = wordTree(word.point(d))
          var f: Float = 0
          var j: Int = 0

          for (j <- 0 to vectorSize-1) {
            f += lastWord.syn0(j) * out.syn1(j)
          }

          if (f > -MAX_EXP && f < MAX_EXP) {
            f = (f + MAX_EXP) * (EXP_TABLE_SIZE / MAX_EXP / 2)
            f = expTable(f.asInstanceOf[Int])
            var g = 1.0f - word.code(d) - f

            for (c <- 0 to vectorSize-1) {
              neu1e(c) += g * out.syn1(c)
            }

            for (c <- 0 to vectorSize-1) {
              tmp = g * lastWord.syn0(c) * out.alpha1(c)
              out.syn1(c) += tmp
              out.delta1(c) += tmp
            }
            out.newSyn1TrainCount += 1
          }
        }

        for (j <- 0 to vectorSize-1) {
          tmp = neu1e(j) * lastWord.alpha0(j)
          lastWord.syn0(j) += tmp
          lastWord.delta0(j) += tmp
        }

        lastWord.newSyn0TrainCount += 1
      }
    }
  }

  def trainWords(index : Int, lines: Iterator[String])  {
    neu1e = new Array[Float](200)

    val items = new mutable.HashMap[Int, NeuronNode]
    trainedWords = 0
    var fetchClient : ParamClient = null
    var pushClient : ParamClient = null

    for (node <- wordTree) {
      node.createVectors()
    }

    println("starting parameter client")
    fetchClient = new ParamClient(this, paramServer, wordTree, ParamService.REQ_FETCH_MODEL, 20)
    fetchClient.connect()

    while (!fetchClient.fetchModel()) {
      Thread.sleep(2000)
    }

    pushClient = new ParamClient(this, paramServer, wordTree, ParamService.REQ_PUSH_GRADIENTS, 1)
    pushClient.connect()

    fetchClient.start()
    pushClient.start()

    var running = true
    var lastTrainedWords = 0L
    var step = 0
    while (running && lines.hasNext) {

      val line = lines.next()

      val sentence = new mutable.MutableList[NeuronNode]
      var tokens = line.split(" ").filter( s => s.length > 0)
      for (token <- tokens) {
        var indexer = wordMap.get(token)
        if (!indexer.isEmpty) {
          val entryIndex = wordMap.get(token).get
          if (entryIndex != -1) {
            val entry = wordTree(entryIndex)
            if (entry != null) {
              var selected = true
              if (sample > 0) {
                val ran = (Math.sqrt(wordTree(entryIndex).frequency / (sample * totalWords)) + 1) * (sample * totalWords) / wordTree(entryIndex).frequency
                nextRandom = nextRandom * 25214903917L + 11
                if (ran < (nextRandom & 0xFFFF) / 65536.0)
                  selected = false
              }

              if (selected) {
                sentence += entry
              }
            }
          }
        }
      }
      if (sentence.size > 0) {
        trainedWords += sentence.size
        totalTrained += sentence.size
        step += 1

        for (sentence_pos <- 0 to sentence.size - 1) {
          nextRandom = nextRandom * 25214903917L + 11
          var b = (nextRandom % windowSize).toInt
          //var b = 3
          skipGram(sentence_pos, sentence, b)
        }
      }
    }

    fetchClient.stopWork()
    pushClient.stopWork()

    fetchClient.join()
    pushClient.join()
  }
}
