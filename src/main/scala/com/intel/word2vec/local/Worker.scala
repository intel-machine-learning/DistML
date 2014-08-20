package com.intel.word2vec.local

import java.nio.ByteBuffer
import java.util.Date
import scala._
import scala.collection.mutable
import scala.Predef._
import com.intel.word2vec.common.FloatOps
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import java.net.URI

/**
 * Created by He Yunlong on 5/28/14.
 */
class Worker(
expTable: Array[Float],
wordTree : Array[NeuronNode],
wordMap : mutable.HashMap[String, Int],
totalWords : Long,
outputFolder : String
) {

  val STEP_SIZE = 10000         // show alpha after these words trained
//  val PARAM_PUSH_STEPS = 3000   // push after these sentences trained
//  val PARAM_FETCH_STEPS = 10000   // fetch after these sentences trained

  final val EXP_TABLE_SIZE: Int = 1000
  final val MAX_EXP: Int = 6

  val sample: Double = 1e-3
  var windowSize: Int = 5
  var vectorSize: Int = 200   //notes there are many hard-coded 200 and 800, change them if needed

  var nextRandom: Long = 5
  var startingAlpha: Float = 0.025f
  var alphaThreshold: Float = 0.0001f

  var trainedWords = 0L
  var alpha = startingAlpha

  var neu1e : Array[Float] = null


  def skipGram(index : Int, sentence: mutable.MutableList[NeuronNode],
               b: Int) {

    val word: NeuronNode = sentence(index)
    var a: Int = 0
    var c: Int = 0

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
            var g =  (1.0f - word.code(d) - f) * alpha

            for (c <- 0 to vectorSize-1) {
              neu1e(c) += g * out.syn1(c)
            }

            for (c <- 0 to vectorSize-1) {
              out.syn1(c) += g * lastWord.syn0(c)
            }
          }
        }

        for (j <- 0 to vectorSize-1) {
          lastWord.syn0(j) += neu1e(j)
        }
      }
    }
  }

  def trainWords(lines: Iterator[String])  {

    neu1e = new Array[Float](200)

    trainedWords = 0

    for (node <- wordTree) {
      node.createVectors()
      node.initVectors(alpha)
    }

    var progress = 0
    while (lines.hasNext) {

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
        alpha = startingAlpha * (1 - trainedWords / (totalWords + 1).toFloat)
        if (alpha < startingAlpha * 0.0001)
          alpha = startingAlpha * 0.0001f

        progress += 1
        if (progress % 10000 == 0)
          println("progress: trainedWords=" + trainedWords + ", totalWords=" + totalWords + ", " + (trainedWords*100.0/totalWords))

        for (sentence_pos <- 0 to sentence.size - 1) {
          nextRandom = nextRandom * 25214903917L + 11
          var b = (nextRandom % windowSize).toInt
          skipGram(sentence_pos, sentence, b)
        }
      }
    }
    saveModelToHdfs()
  }

  def saveModelToHdfs() {
    val writeTime = System.currentTimeMillis()

    var conf = new Configuration()
    val fs = FileSystem.get(URI.create(outputFolder), conf)
    var dst = new Path(outputFolder + "/vectors.bin")
    val out = fs.create(dst)

    val vocabSize = wordTree.length
    var str = "" + vocabSize + " 200\n"
    out.write(str.getBytes("UTF-8"))

    for (a <- 0 to vocabSize - 1) {
      val item = wordTree(a)
      str = item.name + " "
      out.write(str.getBytes("UTF-8"))


        for (b <- 0 to 199) {
          out.write(FloatOps.getFloatBytes(item.syn0(b).toFloat))
        }
      out.writeByte(0x0a)
    }

    out.close()

    println("write done with time " + (System.currentTimeMillis() - writeTime)/1000)
  }
}
