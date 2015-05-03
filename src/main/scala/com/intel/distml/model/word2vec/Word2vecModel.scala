
package com.intel.distml.model.word2vec

/**
 * Created by jichao on 1/15/15.
 */

import java.net.URI
import java.util

import com.intel.distml.api
import com.intel.distml.api.databus.DataBus
import com.intel.distml.api.neuralnetwork.Layer
import com.intel.distml.api.{Partition, DMatrix, PartitionInfo, Model}
import com.intel.distml.model.cnn.ConvKernels
import com.intel.distml.model.sparselr.{LRSample, SparseWeights}
import com.intel.distml.model.word2vec.WordVectorWithAlpha
import com.intel.distml.util._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.MutableList
import scala.util.Random
import scala.collection.mutable.ArrayBuffer
import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.apache.spark.mllib.linalg.{Vector, Vectors}

class Word2VecModel (
val wordTree : WordTree,
wordMap :  mutable.HashMap[String, Int],
dim : Int) extends Model {

  this.autoFetchParams = false

  class ParamMatrix(vocabSize : Int) extends DMatrix(DMatrix.FLAG_PARAM | DMatrix.FLAG_ON_SERVER, vocabSize) {

    setPartitionStrategy(DMatrix.PARTITION_STRATEGY_HASH);

    override def initOnServer(psIndex: Int, keys: KeyCollection) {

      val d = new WordVectorWithAlpha(dim);
      log("create word vectors: " + keys.size() + ", with dim " + dim)
      val nodeData = new GeneralArray[WordVectorWithAlpha](d, keys)

      var size = keys.size().toInt
      for (i <- 0 to size-1) {
        nodeData.values(i) = new WordVectorWithAlpha(dim)
        nodeData.values(i).init(Word2VecModel.ALPHA);
      }
      setLocalCache(nodeData)
    }

//    def autoPartition(psCount : Int): Unit = {
//      serverPartitions = new PartitionInfo(PartitionInfo.Type.PARTITIONED);
//
//      val partSize = (dim + psCount -1) / psCount
//      for (i <- 0 to psCount-1) {
//        var first = partSize * i
//        var last = first + partSize - 1
//        if (last >= dim) {
//          last = dim - 1;
//        }
//        var p = new Partition(new KeyRange(first, last));
//        serverPartitions.addPartition(p)
//      }
//    }
  }

  class UpdateMatrix(vocabSize : Int) extends DMatrix(DMatrix.FLAG_ON_SERVER, vocabSize) {

    override def initOnServer(psIndex: Int, keys: KeyCollection) {
      val d = new WordVectorUpdate(dim);
      val nodeData = new GeneralArray[WordVectorUpdate](d, keys)

      var size = keys.size().toInt
      for (i <- 0 to size-1) {
        nodeData.values(i) = new WordVectorUpdate(dim)
        nodeData.values(i).init();
      }
      setLocalCache(nodeData)
    }

    override def initOnWorker(psIndex: Int, keys: KeyCollection) {
    }

//    def autoPartition(psCount : Int): Unit = {
//      serverPartitions = new PartitionInfo(PartitionInfo.Type.PARTITIONED);
//
//      val partSize = (dim + psCount -1) / psCount
//      for (i <- 0 to psCount-1) {
//        var first = partSize * i
//        var last = first + partSize - 1
//        if (last >= dim) {
//          last = dim - 1;
//        }
//        var p = new Partition(new KeyRange(first, last));
//        serverPartitions.addPartition(p)
//      }
//    }
  }

  @transient var expTable: Array[Float] = null;
  @transient var neu1 : TempNodeData = null
  @transient var neu1e : TempNodeData = null
  var nextRandom: Long = 5

  locally {
    var params = new ParamMatrix(wordTree.vocabSize);
    params.setPartitionStrategy(DMatrix.PARTITION_STRATEGY_HASH)
    registerMatrix(Model.MATRIX_PARAM, params)
    var updates = new UpdateMatrix(wordTree.vocabSize);
    updates.setPartitionStrategy(DMatrix.PARTITION_STRATEGY_HASH)
    registerMatrix(Model.MATRIX_UPDATE, updates)

  }

  override def transformSamples (samples: java.util.List[AnyRef]) : Matrix = {
    println("transform samples: " + samples.size());
    return new Blob[java.util.List[AnyRef]](samples)
    //return samples.get(0).asInstanceOf[Matrix]
  }

  private def log(msg: String) {
    Logger.InfoLog("****************** " + msg + " ******************", Logger.Role.APP, 0)
  }
//
//
//  private def autoPartition(psCount : Int) {
//
//    val paramMatrix = getMatrix(Model.MATRIX_PARAM).asInstanceOf[ParamMatrix]
//    paramMatrix.autoPartition(psCount)
//
//    val updateMatrix = getMatrix(Model.MATRIX_UPDATE).asInstanceOf[UpdateMatrix]
//    updateMatrix.autoPartition(psCount)
//  }

  private def init {
    expTable = new Array[Float](Word2VecModel.EXP_TABLE_SIZE)
    var i: Int = 0
    for (i <- 0 to Word2VecModel.EXP_TABLE_SIZE-1) {
      expTable(i) = math.exp(((i / Word2VecModel.EXP_TABLE_SIZE.asInstanceOf[Float] * 2 - 1) * Word2VecModel.MAX_EXP)).toFloat
      expTable(i) = expTable(i) / (expTable(i) + 1)
    }

    neu1 = new TempNodeData(dim)
    neu1e = new TempNodeData(dim)
  }

  override def mergeUpdate(serverIndex: Int, matrixName: String, update: Matrix): Unit = {
    val params = getMatrix(Model.MATRIX_PARAM).localCache.asInstanceOf[GeneralArray[WordVectorWithAlpha]]
    val deltas = getMatrix(Model.MATRIX_UPDATE).localCache.asInstanceOf[GeneralArray[WordVectorUpdate]]
    println("delta row keys: " + deltas.rowKeys);

    val u = update.asInstanceOf[HashMapMatrix[WordVectorUpdate]].data
    for (key <- u.keySet()) {
      val item = u.get(key)
      val p = params.element(key.intValue)
      val d = deltas.element(key.intValue)

      for (i <- 0 to Word2VecModel.vectorSize -1) {
        val delta = item.delta0(i)
        p.syn0(i) += delta
        d.delta0(i) += delta * delta
        p.alpha0(i) = Word2VecModel.initialAlpha / Math.sqrt(1.0 + d.delta0(i)).toFloat
        if (p.alpha0(i) <= Word2VecModel.alphaThreshold)
          p.alpha0(i) = Word2VecModel.alphaThreshold
      }

      for (i <- 0 to Word2VecModel.vectorSize -1) {
        val delta = item.delta1(i)
        p.syn1(i) += delta
        d.delta1(i) += delta * delta
        p.alpha1(i) = Word2VecModel.initialAlpha / Math.sqrt(1.0 + d.delta1(i)).toFloat
        if (p.alpha1(i) <= Word2VecModel.alphaThreshold)
          p.alpha1(i) = Word2VecModel.alphaThreshold
      }
    }
  }

  def prefetch(sentences: java.util.Collection[String], wordTree : WordTree, wordMap : mutable.HashMap[String, Int]): KeyList = {

    val keyList = new KeyList();

    for(line <- sentences) {
      //val sentence = new mutable.MutableList[Int]
      var tokens = line.split(" ").filter( s => s.length > 0)
      for (token <- tokens) {
        var indexer = wordMap.get(token)
        if (!indexer.isEmpty) {
          val entryIndex = wordMap.get(token).get
          if (entryIndex != -1) {
            keyList.addKey(entryIndex);

            var w = wordTree.getWord(entryIndex)
            for (d <- 0 to w.codeLen-1) {
              keyList.addKey(w.point(d))
            }
          }
        }
      }
    }

    keyList
  }


  override def compute(s: Matrix, workerIndex: Int, dataBus: DataBus) {

    if (expTable == null)
      init

    var sentences = s.asInstanceOf[Blob[util.LinkedList[String]]].element()
    //println("sentences: " + sentences);
    val keyList = prefetch(sentences, wordTree, wordMap);

    log("prefetch: " + keyList.size())
    val dataMap : HashMapMatrix[WordVectorWithAlpha] = dataBus.fetchFromServer(Model.MATRIX_PARAM, keyList).asInstanceOf[HashMapMatrix[WordVectorWithAlpha]];
    val updateMap = createUpdateMap(dataMap)

    log("training sentences: " + sentences.size() + ", " + dataMap.getRowKeys.size());

    for (line <- sentences) {
      //println("train with line: " + line)
      val sentence = new mutable.MutableList[WordNode]
      var tokens = line.split(" ").filter( s => s.length > 0)
      for (token <- tokens) {
        var indexer = wordMap.get(token)
        if (!indexer.isEmpty) {
          val entryIndex = wordMap.get(token).get
          if (entryIndex != -1) {
            val entry = wordTree.getWord(entryIndex)
            if (entry != null) {
              //println("train entry index: " + entryIndex)
              sentence += entry
              //trainedWords += 1
            }
          }
        }
      }
      //trainedWords += tokens.size

      for (sentence_pos <- 0 to sentence.size - 1) {
        nextRandom = nextRandom * 25214903917L + 11
        var b = (nextRandom % Word2VecModel.windowSize).toInt
        //var b = 3
        cbow(wordTree, dataMap, updateMap, sentence_pos, sentence, b)
        //skipGram(sentence_pos, sentence, b)
      }
    }

    dataBus.pushUpdate(Model.MATRIX_UPDATE, updateMap)
  }

  def createUpdateMap(vectorMap : HashMapMatrix[WordVectorWithAlpha]): HashMapMatrix[WordVectorUpdate] = {
    val updateMap = new HashMapMatrix[WordVectorUpdate]();
    for (key <- vectorMap.data.keySet()) {
      updateMap.put(key, new WordVectorUpdate(Word2VecModel.vectorSize));
    }

    updateMap
  }

  def cbow(wordTree : WordTree,
           dataMap : HashMapMatrix[WordVectorWithAlpha],
           updateMap : HashMapMatrix[WordVectorUpdate],
           index: Int,
           sentence: MutableList[WordNode],
           b: Int) {

    //log("cbow");

    val word = sentence(index)
    var a: Int = 0
    var c: Int = 0
    var tmp = 0.0f

    neu1.clear()
    neu1e.clear()

    /* for (a = b; a < window * 2 + 1 - b; a++) if (a != window) {
      c = sentence_position - window + a;
      if (c < 0) continue;
      if (c >= sentence_length) continue;
      last_word = sen[c];
      if (last_word == -1) continue;
      for (c = 0; c < layer1_size; c++) neu1[c] += syn0[c + last_word * layer1_size];
    } */

    for (a <- b to Word2VecModel.windowSize * 2 - b) {
      c = index - Word2VecModel.windowSize + a
      if ((a != Word2VecModel.windowSize) && (c >= 0 && c < sentence.size)) {
        val lastWord = dataMap.get(sentence(c).index);
        neu1.addFromSyn0(lastWord)
      }
    }

    for (d <- 0 to word.codeLen-1) {
      val out = dataMap.get(wordTree.getWord(word.point(d)).index)
      val outUpdate = updateMap.get(wordTree.getWord(word.point(d)).index)
      var f: Float = neu1.dotProductSyn1(out)

      if (f > -Word2VecModel.MAX_EXP && f < Word2VecModel.MAX_EXP) {
        f = (f + Word2VecModel.MAX_EXP) * (Word2VecModel.EXP_TABLE_SIZE / Word2VecModel.MAX_EXP / 2)
        f = expTable(f.asInstanceOf[Int])
        val g = 1.0f - word.code(d) - f
        neu1e.accumSyn1(out, g)
        out.calculateDelta1(neu1, g, outUpdate)
      }
    }

    for (a <- b to Word2VecModel.windowSize * 2 - b) {
      c = index - Word2VecModel.windowSize + a
      if ((a != Word2VecModel.windowSize) && (c >= 0 && c < sentence.size)) {
        val lastWord = dataMap.get(sentence(c).index);
        val lastWordUpdate = updateMap.get(sentence(c).index);
        lastWord.calculateDelta0(neu1e, lastWordUpdate)
      }
    }
  }

  def skipGram(wordTree : WordTree,
               dataMap : HashMapMatrix[WordVectorWithAlpha],
               updateMap : HashMapMatrix[WordVectorUpdate],
               index: Int,
               sentence: MutableList[WordNode],
               b: Int) {

    val word = sentence(index)
    var a: Int = 0
    var c: Int = 0
    var tmp = 0.0f

    //    var neu1 = new TempNodeData()
    var neu1e = new TempNodeData(dim)


    for (a <- b to Word2VecModel.windowSize * 2 - b) {
      c = index - Word2VecModel.windowSize + a
      if ((a != Word2VecModel.windowSize) && (c >= 0 && c < sentence.size)) {

        val lastWord = dataMap.get(sentence(c).index)
        val lastWordUpdate = updateMap.get(sentence(c).index);

        neu1e.clear()

        for (d <- 0 to word.codeLen-1) {
          val out = dataMap.get(wordTree.getWord(word.point(d)).index)
          val outUpdate = updateMap.get(wordTree.getWord(word.point(d)).index)
          //          if (lastWord == null)
          //            Utils.debug("" + Worker.this + ". data fail: " + sentence(c).index)
          //          if (out == null)
          //            Utils.debug("" + Worker.this + ". data fail: " + word.point(d))

          var f: Float = lastWord.f(out)

          if (f > -Word2VecModel.MAX_EXP && f < Word2VecModel.MAX_EXP) {
            f = (f + Word2VecModel.MAX_EXP) * (Word2VecModel.EXP_TABLE_SIZE / Word2VecModel.MAX_EXP / 2)
            f = expTable(f.asInstanceOf[Int])
            val g = 1.0f - word.code(d) - f

            neu1e.accumSyn1(out, g)
            out.calculateDelta1(lastWord, g, outUpdate)
          }
        }

        lastWord.calculateDelta0(neu1e, lastWordUpdate)
      }
    }
  }
}

object Word2VecModel {

  val minFreq = 5;

  val windowSize = 7
  val vectorSize = 200

  val initialAlpha = 0.0025f
  val alphaThreshold = 0.0001f

  val ALPHA = 0.1f

  val MAX_CODE_LENGTH = 50

  final val EXP_TABLE_SIZE: Int = 1000
  final val MAX_EXP: Int = 6

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
    var code = new Array[Int](Word2VecModel.MAX_CODE_LENGTH)
    var point = new Array[Int](Word2VecModel.MAX_CODE_LENGTH)

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

  def saveToHDFS(outputFolder:String, model : Word2VecModel): Unit = {
    println("writint word2vec model to hdfs...")
    // =================== write model to HDFS =======================
    val params = model.getMatrix(Model.MATRIX_PARAM).localCache.asInstanceOf[GeneralArray[WordVectorWithAlpha]]

    val fs = FileSystem.get(URI.create(outputFolder), new Configuration())
    var dst = new Path(outputFolder + "/model.bin")
    val out = fs.create(dst)

    var str = "" + model.wordTree.vocabSize + " " + Word2VecModel.vectorSize + "\n"
    out.write(str.getBytes("UTF-8"))
    println("vocabulary: " + str)

    var dataBytes = new Array[Byte](Word2VecModel.vectorSize * 4)
    for (wordIndex <- 0 to model.wordTree.vocabSize - 1) {
      println("write: " + wordIndex)

      val w = model.wordTree.getWord(wordIndex)
      val str = w.name + " "
      out.write(str.getBytes("UTF-8"))

      val data = params.element(wordIndex)
      for (i <- 0 to Word2VecModel.vectorSize-1) {
        getFloatBytes(data.syn0(i), dataBytes, i*4)
      }
      out.write(dataBytes)
      out.writeByte(0x0a)
    }

    out.close()
    fs.close()

    System.out.println("===== model has been written to HDFS ====")

  }


  def getWord2VecMap( model : Word2VecModel): Word2VecModelAPI = {

    val word2VecMap = mutable.HashMap.empty[String, Array[Float]]
    val params = model.getMatrix(Model.MATRIX_PARAM).localCache.asInstanceOf[GeneralArray[WordVectorWithAlpha]]
    println("get word map vec : String -> Float: " + model.wordTree.vocabSize + ", " + params.rowKeys.size())

    for (wordIndex <- 0 to model.wordTree.vocabSize - 1) {
      //println("write: " + wordIndex)

      val w = model.wordTree.getWord(wordIndex)
      val word = w.name
      val vector = new Array[Float](vectorSize)
      val data = params.element(wordIndex)
      for (i <- 0 to Word2VecModel.vectorSize-1)
       vector(i) = data.syn0(i)
      word2VecMap += word -> vector
    }

    new Word2VecModelAPI(word2VecMap.toMap)

  }

  def getFloatBytes(fvalue : Float, data : Array[Byte], offset : Int) {
    var ivalue = java.lang.Float.floatToIntBits(fvalue);
    data(offset+3) = ((ivalue >> 24) & 0xff).asInstanceOf[Byte];
    data(offset+2) = ((ivalue >> 16) & 0xff).asInstanceOf[Byte];
    data(offset+1) = ((ivalue >>  8) & 0xff).asInstanceOf[Byte];
    data(offset) = (ivalue & 0xff).asInstanceOf[Byte];
  }
}

  class Word2VecModelAPI  (
                                       private val model: Map[String, Array[Float]]) extends Serializable {
    private def cosineSimilarity(v1: Array[Float], v2: Array[Float]): Double = {
      require(v1.length == v2.length, "Vectors should have the same length")
      val n = v1.length
      val norm1 = blas.snrm2(n, v1, 1)
      val norm2 = blas.snrm2(n, v2, 1)
      if (norm1 == 0 || norm2 == 0) return 0.0
      blas.sdot(n, v1, 1, v2,1) / norm1 / norm2
    }

    def transform(word: String): Vector = {
      model.get(word) match {
        case Some(vec) =>
          Vectors.dense(vec.map(_.toDouble))
        case None =>
          throw new IllegalStateException(s"$word not in vocabulary")
      }
    }

    def findSynonyms(word: String, num: Int): Array[(String, Double)] = {

      val vector = transform(word)
      findSynonyms(vector,num)
    }

    def findSynonyms(vector: Vector, num: Int): Array[(String, Double)] = {
      require(num > 0, "Number of similar words should > 0")
      // TODO: optimize top-k
      val fVector = vector.toArray.map(_.toFloat)
      model.mapValues(vec => cosineSimilarity(fVector, vec))
        .toSeq
        .sortBy(- _._2)
        .take(num + 1)
        .tail
        .toArray
    }
  }

