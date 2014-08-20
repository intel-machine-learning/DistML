package com.intel.word2vec.clusterps

import java.io._
import java.net._
import scala.collection.mutable
import com.intel.word2vec.common.{Utils, IOHelper, FloatOps}

/**
 * Created by He Yunlong on 7/12/14.
 */
class Fetcher (
partitionIndex : Int,
val server : ServerInfo,
worker : Worker
) extends Thread {

  //var queue = new mutable.Queue[Int]()
  var queue = new mutable.HashSet[Int]

  var socket : Socket = null
  var dis : DataInputStream = null
  var dos : DataOutputStream = null

  var running = false
  var idle = true
  var thread : Thread = null

  var writer = new Writer()
  var reader = new Reader()

  def connect() {
    socket = new Socket(server.address, server.fetchServicePort)
    socket.setSoTimeout(1000000)
    dis = new DataInputStream(socket.getInputStream())
    dos = new DataOutputStream(socket.getOutputStream())
    //println("connected")
  }

  def disconnect() {
    dis.close()
    dos.close()
    socket.close()
  }

  def addIndex(index : Int) : Boolean = {
    //println("addindex: " + index + ", " + server.fromIndex + ", " + server.toIndex)
    if ((index >= server.fromIndex) && (index <= server.toIndex)) {
      //queue.enqueue(index)
      queue.add(index)

      return true
    }
    return false
  }

  def startFetch() {
    //println("start fetch now: " + queue.size + " from " + server.address)
    idle = false
  }

  def stopWork() {
    running = false
  }

  override def run() {
    connect()

    idle = true
    running = true
    while (running) {

      if (!idle) {
        if (queue.size > 0) {
          dos.writeInt(Constants.FETCH_NEW_FETCH_INDICATOR)

          reader.fetchCount = queue.size

          var t1 = new Thread(writer)
          var t2 = new Thread(reader)
          t1.start()
          t2.start()
          t1.join()
          t2.join()

          Utils.debug("fetch done from " + server.address)
        }
        //println("queue is empty now " + server.address)
        idle = true
      }
      Thread.sleep(10)
    }

    dos.writeInt(Constants.FETCH_CLOSE_INDICATOR)

    disconnect()
  }

  class Writer extends Runnable {
    override def run() {
      var count = queue.size
      //println("fetching with count: " + count + " from: " + server.address)
      var it = queue.toIterator

      dos.writeInt(partitionIndex)
      dos.writeInt(count)

      while(it.hasNext) {
        var i = it.next()
        dos.writeInt(i)
      }
      queue.clear()
    }
  }

  class Reader extends Runnable {

    var fetchCount = 0

    override def run() {
      var dataBytes = new Array[Byte](Constants.MODEL_DIMENSION * 4)

      for (i <- 0 to fetchCount-1) {
        var d = worker.bufferedPool.getFreeData()

        var index = IOHelper.readInt(dis)
        //println("fetched: " + index)
        d.index = index

        dis.readFully(dataBytes)
        for (i <- 0 to Constants.MODEL_DIMENSION-1) {
          d.syn0(i) = FloatOps.getFloatFromBytes(dataBytes, i*4)
        }
        dis.readFully(dataBytes)
        for (i <- 0 to Constants.MODEL_DIMENSION-1) {
          d.syn1(i) = FloatOps.getFloatFromBytes(dataBytes, i*4)
        }

        dis.readFully(dataBytes)
        for (i <- 0 to Constants.MODEL_DIMENSION-1) {
          d.alpha0(i) = FloatOps.getFloatFromBytes(dataBytes, i*4)
        }
        dis.readFully(dataBytes)
        for (i <- 0 to Constants.MODEL_DIMENSION-1) {
          d.alpha1(i) = FloatOps.getFloatFromBytes(dataBytes, i*4)
        }
      }
    }
  }

}
