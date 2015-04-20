package com.intel.word2vec.singleps


import java.io.{DataOutputStream, DataInputStream}
import java.net.{Socket, ServerSocket}
import java.nio.ByteBuffer
import com.intel.word2vec.common.FloatOps

/**
 * Created by He Yunlong on 5/25/14.
 */
class ParamClient (
worker : Worker,
paramServer : String,
wordTree : Array[NeuronNode],
cmd : Int,
interval : Int
) extends Thread {

  var running = true

  var socket: Socket = null
  var dis : DataInputStream = null
  var dos : DataOutputStream = null

  def stopWork() {
    println("stopping client thread")
    running = false
  }

  def connect() {

    socket = new Socket(paramServer, 9989)
    dis = new DataInputStream(socket.getInputStream())
    dos = new DataOutputStream(socket.getOutputStream())

  }

  override def run() {

    try {
      while (running) {
        if (cmd == ParamService.REQ_PUSH_GRADIENTS) {
          pushDelta()
        }
        else if (cmd == ParamService.REQ_FETCH_MODEL) {
          fetchModel()
        }

        Thread.sleep(interval*1000)
      }
    }
    catch {
      case e : Exception => e.printStackTrace()
    }

    try {
      dis.close()
      dos.close()
      socket.close()
    }
    catch { case xe : Exception => xe.printStackTrace()}

    println("client thread stopped")
  }

  def pushDelta() : Boolean = {

    dos.write(ParamService.REQ_PUSH_GRADIENTS)
    if (dis.read() != ParamService.RES_OK) {
      println("server busy, try later")
      return false
    }

    println("push gradient start: " + worker.partitionIndex)

    val writeTime = System.currentTimeMillis()

    dos.writeInt(worker.partitionIndex)
    dos.writeInt(wordTree.length)
    dos.writeLong(worker.trainedWords)
    var i, j = 0
    var item : NeuronNode = null

    var dataBytes = new Array[Byte](800)

    var writeCount = 0
    for ( i <- 0 to wordTree.length-1 ) {
      item = wordTree(i)
      var skip = false
      if ((item.newSyn0TrainCount == 0)
          && (item.newSyn1TrainCount == 0)) {
          skip = true
      }

      if (!skip) {
        writeCount += 1

        dos.writeInt(item.index)

        for (i <- 0 to 199) {
          FloatOps.getFloatBytes(item.delta0(i), dataBytes, i*4)
          item.delta0(i) = 0.0f
        }
        dos.write(dataBytes)

        for (i <- 0 to 199) {
          FloatOps.getFloatBytes(item.delta1(i), dataBytes, i*4)
          item.delta1(i) = 0.0f
        }
        dos.write(dataBytes)
      }

      item.newSyn0TrainCount = 0
      item.newSyn1TrainCount = 0
    }
    dos.writeInt(ParamService.IND_PUSH_DONE)

    println("push gradient done with time " + (System.currentTimeMillis() - writeTime)/1000 + ", count=" + writeCount)

    return true
  }

  def fetchModel() : Boolean = {

    dos.write(ParamService.REQ_FETCH_MODEL)
    if (dis.read() != ParamService.RES_OK) {
      println("server busy, try later")
      return false
    }

    val startTime = System.currentTimeMillis()
    var vocabSize = dis.readInt()
    worker.totalTrained = dis.readLong()
    worker.alpha = dis.readFloat()
    println("fetch model, vocabSize=" + vocabSize)

    readParameters(vocabSize)

    println("fetch model done with time " + (System.currentTimeMillis() - startTime)/1000 + "\n")

    return true
  }

  def readParameters(paramCount : Int) {
    var dataBytes : Array[Byte] = new Array[Byte](800)

    var i, j = 0
    var oneParamSize = 4 + 1600   // index, syn0(200) syn1(200)
    for (i <- 0 to paramCount-1) {
      while (dis.available() < oneParamSize) {
        Thread.sleep(1)
      }

      var index = dis.readInt()
      var item = wordTree(index)

        dis.readFully(dataBytes)
        for (i <- 0 to 199) {
          //item.syn0(i) = dis.readFloat()
          item.syn0(i) = FloatOps.getFloatFromBytes(dataBytes, i * 4)
        }

        dis.readFully(dataBytes)
        for (i <- 0 to 199) {
          item.syn1(i) = FloatOps.getFloatFromBytes(dataBytes, i * 4)
        }

        dis.readFully(dataBytes)
        for (i <- 0 to 199) {
          item.alpha0(i) = FloatOps.getFloatFromBytes(dataBytes, i * 4)
        }
        dis.readFully(dataBytes)
        for (i <- 0 to 199) {
          item.alpha1(i) = FloatOps.getFloatFromBytes(dataBytes, i * 4)
        }
      }
  }
}