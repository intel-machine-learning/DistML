package com.intel.word2vec.clusterps

import java.io._
import java.net._
import scala.collection.mutable
import com.intel.word2vec.common.{IOHelper, FloatOps}

/**
 * Created by He Yunlong on 7/12/14.
 */
class Pusher (
server : ServerInfo,
worker : Worker
) extends Thread {

  var queue = new mutable.Queue[W2VWorkerNodeData]()
  var dataBytes = new Array[Byte](Constants.MODEL_DIMENSION * 4)

  var socket : Socket = null
  var dis : DataInputStream = null
  var dos : DataOutputStream = null

  var running = false
  var idle = true
  var thread : Thread = null

  def addIndex(d : W2VWorkerNodeData) : Boolean = {
    if (server.accept(d.deltaIndex)) {
      queue.enqueue(d)
      return true
    }
    return false
  }

  def connect() {
    socket = new Socket(server.address, server.pushServicePort)
//    println("send buffer" + socket.getSendBufferSize())
//    println("receive buffer" + socket.getReceiveBufferSize())
//    socket.setSendBufferSize(Constants.MODEL_DIMENSION * 64)
//    socket.setReceiveBufferSize(Constants.MODEL_DIMENSION * 64)
//    println("send buffer" + socket.getSendBufferSize())
//    println("receive buffer" + socket.getReceiveBufferSize())

    dis = new DataInputStream(socket.getInputStream())
    dos = new DataOutputStream(socket.getOutputStream())

  }

  def disconnect() {
    dis.close()
    dos.close()
    socket.close()
  }

  def startPush() {
    //println("start push now: " + queue.size)
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
          var count = 0
          var total = queue.size
          //println("push started: " + server.address + ", " + total)
          dos.writeInt(Constants.PUSH_NEW_DATA_INDICATOR)
          dos.writeInt(total)

          var it = queue.toIterator
          while(it.hasNext) {
            var t = it.next()
            push(t)
            count += 1
          }
          queue.clear()

          var ind = IOHelper.readInt(dis)  // Constants.PUSH_DONE_INDICATOR
          //println("push done: " + server.address)
        }
        idle = true
      }
      Thread.sleep(10)
    }

    dos.writeInt(Constants.PUSH_CLOSE_INDICATOR)

    disconnect()
  }

  def push(d : W2VWorkerNodeData) {
    dos.writeInt(d.deltaIndex)
    for (i <- 0 to Constants.MODEL_DIMENSION - 1) {
      FloatOps.getFloatBytes(d.delta0(i), dataBytes, i*4)
    }
    dos.write(dataBytes)
    for (i <- 0 to Constants.MODEL_DIMENSION - 1) {
      FloatOps.getFloatBytes(d.delta1(i), dataBytes, i*4)
    }
    dos.write(dataBytes)
  }

}
