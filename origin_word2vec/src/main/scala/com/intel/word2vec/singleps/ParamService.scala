package com.intel.word2vec.singleps

import java.io.{IOException, DataOutputStream, DataInputStream}
import java.lang.Thread
import java.net._
import java.nio.ByteBuffer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import scala.collection.mutable
import com.intel.word2vec.common.FloatOps
import com.intel.word2vec.clusterps.Constants

/**
 * Created by He Yunlong on 5/25/14.
 */
class ParamService (
outputFolder : String,
wordTree : Array[NeuronNode],
totalWords : Long,
alpha : Float
) extends Thread {

  var running = true
  var clients = new mutable.HashMap[Int, ServerThread]()
  var workingList = new mutable.HashSet[ServerThread]()

  val WORKING_LIST_LIMIT = 8

  def stopWork() {
    running = false
  }

  def initModel() {
    println("init vectors with: " + alpha)
    for (node <- wordTree) {
      node.createVectors()
      node.initVectors(alpha)
    }
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

  def getAlpha() : Float = {
    //synchronized(clients) {
      var trained = getTrainedWords

      var newAlpha = alpha * (1 - trained / (totalWords + 1).toFloat)
      if (newAlpha < alpha * 0.0001) newAlpha = alpha * 0.0001f

      println("===alpha: " + newAlpha + ", " + trained + ", " + totalWords)
      newAlpha
    //}
  }

  def getTrainedWords() : Long = {
    //synchronized(clients) {
    var trained = 0L
    for (c <- clients.values) {
      trained += c.trainedWords
    }
    trained
    //}
  }

  def addServerThread(thread : ServerThread, partitionIndex : Int) {
    clients.put(partitionIndex, thread)
  }

  def accquireLock(thread : ServerThread) : Boolean = {
    if (workingList.size < WORKING_LIST_LIMIT) {
      workingList += thread
      return true
    }

    println("ignore request ...")
    return false
  }

  def releaseLock(thread : ServerThread) {
    workingList -= thread
  }

  override def run() {
    val ss: ServerSocket = new ServerSocket(9989)
    ss.setSoTimeout(10000)

    while (running) {
      try {
        val s: Socket = ss.accept()   ///192.168.42.103:37493
        var host = s.getInetAddress.getHostName
        println("connection established from : " + s.getInetAddress.getHostName)

        var client = new ServerThread(this, wordTree, alpha, s)

        //clients.put(s.getRemoteSocketAddress.toString, client)
        //clients += client
        client.start()
      }
      catch {
        case toe: SocketTimeoutException =>
      }
    }

    ss.close()

    saveModelToHdfs
    println("server thread stopped")
  }
}

object ParamService {

  val REQ_FETCH_MODEL = 1

  val REQ_PUSH_GRADIENTS = 2

  val CMD_EXIT = 4
  
  val RES_OK = 1
  val RES_BUSY = 2

  val IND_PUSH_DONE = 1000000
}

class ServerThread (
service : ParamService,
wordTree : Array[NeuronNode],
initialAlpha : Float,
socket : Socket,
var trainedWords : Long = 0L
) extends Thread {

  var dis : DataInputStream = new DataInputStream(socket.getInputStream())
  var dos : DataOutputStream = new DataOutputStream(socket.getOutputStream())

  val alphaThreshold: Float = 0.0001f

  override def run() {
    try {
      var running = true
      while(running) {
        val cmd = dis.read()

        dos.write(ParamService.RES_OK)
        if (cmd == ParamService.REQ_FETCH_MODEL) {
          writeModel()
        }
        else if (cmd == ParamService.REQ_PUSH_GRADIENTS) {
          readDelta()
        }
      }
    }
    catch {
      case e: Exception =>
    }

    try {
      dis.close()
      dos.close()
      socket.close()
    }
    catch { case xe : Exception => }
  }


  def readDelta() {
    val startTime = System.currentTimeMillis()

    val partitionIndex = dis.readInt()
    println("\n read grads from partition: " + partitionIndex)

    var vocabSize = dis.readInt()
    trainedWords = dis.readLong()
    service.addServerThread(this, partitionIndex)

    var item : NeuronNode = null

    var dataBytes = new Array[Byte](800)

    var count = 0
    val oneParamSize = 1600   // index, syn0(200) syn1(200)
    var index = dis.readInt()
    while (index != ParamService.IND_PUSH_DONE) {
      count += 1
      while (dis.available() < oneParamSize) {  // index has been read
        Thread.sleep(1)
      }
      item = wordTree(index)

      dis.readFully(dataBytes)
      for (i <- 0 to 199) {
        val delta = FloatOps.getFloatFromBytes(dataBytes, i * 4)
        item.syn0(i) += delta
        item.delta0(i) += delta * delta
        if (item.delta0(i) > 1.0) {
          item.alpha0(i) = initialAlpha / Math.sqrt(item.delta0(i)).toFloat
          if (item.alpha0(i) <= Constants.alphaThreshold)
            item.alpha0(i) = Constants.alphaThreshold
        }
      }

      dis.readFully(dataBytes)
      for (i <- 0 to 199) {
        val delta = FloatOps.getFloatFromBytes(dataBytes, i * 4)
        item.delta1(i) += delta * delta
        if (item.delta1(i) > 1.0) {
          item.alpha1(i) = initialAlpha / Math.sqrt(item.delta1(i)).toFloat
          if (item.alpha1(i) <= Constants.alphaThreshold)
            item.alpha1(i) = Constants.alphaThreshold
        }
        item.syn1(i) += delta
      }

      index = dis.readInt()
    }

    println("read done with time " + (System.currentTimeMillis() - startTime)/1000 + " count=" + count + "\n")
  }

  def writeModel() {

    val startTime = System.currentTimeMillis()

    var vocabSize = wordTree.length
    println("\nwrite model start,  vocabSize=" + vocabSize)

    dos.writeInt(vocabSize)
    dos.writeLong(service.getTrainedWords())
    dos.writeFloat(service.getAlpha())
    var i, j = 0
    var item : NeuronNode = null

    var dataBytes = new Array[Byte](800)
    for (i <- 0 to vocabSize-1) {
      item = wordTree(i)
      dos.writeInt(item.index)

        for (i <- 0 to 199) {
          FloatOps.getFloatBytes(item.syn0(i), dataBytes, i*4)
        }
        dos.write(dataBytes)

        for (i <- 0 to 199) {
          FloatOps.getFloatBytes(item.syn1(i), dataBytes, i*4)
        }
        dos.write(dataBytes)

        for (i <- 0 to 199) {
          FloatOps.getFloatBytes(item.alpha0(i), dataBytes, i*4)
        }
        dos.write(dataBytes)

        for (i <- 0 to 199) {
          FloatOps.getFloatBytes(item.alpha1(i), dataBytes, i*4)
        }
        dos.write(dataBytes)
      }

    println("===write done with time " + (System.currentTimeMillis() - startTime)/1000 + "\n")
  }
}

