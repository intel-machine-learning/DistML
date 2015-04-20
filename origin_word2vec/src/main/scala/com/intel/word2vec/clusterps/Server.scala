package com.intel.word2vec.clusterps

import scala.collection.mutable.LinkedList
import java.net.{SocketException, SocketTimeoutException, Socket, ServerSocket}
import scala.collection.mutable
import java.io.{DataOutputStream, DataInputStream}
import scala.util.Random
import com.intel.word2vec.common.{Utils, IOHelper, FloatOps}

/**
 * Created by He Yunlong on 7/12/14.
 */
class Server(
driverAddress : String,
paramServerNetworkPrefix :String,
vocabSize : Int
) {

  var serverIndex = 0

  var pushServicePort: Int = 0
  var fetchServicePort: Int = 0

  var partition : Array[WordNodeData] = null

  var running = true

  def stopWork() {
    running = false
  }

  var socket : Socket = null
  var dis : DataInputStream = null
  var dos : DataOutputStream = null

  def getData(index : Int) : WordNodeData = {
    return partition(index / Constants.PARAM_SERVER_COUNT)
  }

  def sendInfoToMonitor() {
    println("init parameter server: " + driverAddress)

    socket = new Socket(driverAddress, Constants.MONITOR_PORT)
    dis = new DataInputStream(socket.getInputStream())
    dos = new DataOutputStream(socket.getOutputStream())

    dos.writeInt(Constants.NODE_TYPE_PARAM_SERVER)
    dos.writeUTF(Utils.getLocalIP(paramServerNetworkPrefix))
    dos.writeInt(pushServicePort)
    dos.writeInt(fetchServicePort)

    serverIndex = IOHelper.readInt(dis)
    println("serverIndex: " + serverIndex)

    partition = new Array[WordNodeData](vocabSize/Constants.PARAM_SERVER_COUNT + 1)

    var d : WordNodeData = null
    for (i <- 0 to partition.length - 1) {
      d = new WordNodeData(Constants.MODEL_DIMENSION)
      d.index = i * Constants.PARAM_SERVER_COUNT + serverIndex
      d.init(Constants.initialAlpha)
      partition(i) = d
      //wordTree.getWord(i).data = d
    }
    val rt = Runtime.getRuntime();
    val usedMemory = rt.totalMemory() - rt.freeMemory();
    println("init done, useMemory=" + usedMemory)

    dos.writeInt(1)
    println("param server ready, serverIndex: " + serverIndex)
  }

  def start() {
    val rt = Runtime.getRuntime();
    val usedMemory = rt.totalMemory() - rt.freeMemory();
    println("starting parameter server" + ", useMemory=" + usedMemory)

    var p = new PushService()
    var f = new FetchService()
    sendInfoToMonitor()

    p.start()
    f.start()

    // waiting "work done" notification
    var workDoneIndicator = dis.read()
    println("workers have done their work, stopping myself")

//    if (fromIndex < vocabSize) {
      //var end = Math.min(toIndex, vocabSize-1)
      //var count = end - fromIndex + 1
      //dos.writeInt(count)
      //println("param count: " + count)

      var dataBytes = new Array[Byte](Constants.MODEL_DIMENSION * 4)
      for (i <- 0 to partition.length - 1) {
        var d = partition(i)
        if (d.index < vocabSize) {
          dos.writeInt(d.index)
          for (i <- 0 to Constants.MODEL_DIMENSION-1) {
            FloatOps.getFloatBytes(d.syn0(i), dataBytes, i*4)
          }
          dos.write(dataBytes)
        }
      }
/*
    }
    else {
      dos.writeInt(0)
    }
*/

    dos.writeInt(-1)

    println("stopping fetch/push service: " )
    stopWork()

    p.stopWork()
    f.stopWork()

    p.join()
    f.join()

    println("param server done: " )
  }

  class PushService extends Thread {
    val ss: ServerSocket = new ServerSocket(0)
    ss.setSoTimeout(10000)
    pushServicePort = ss.getLocalPort

    def stopWork() {
      ss.close()
    }

    override def run() {

      while (running) {
        try {
          val s: Socket = ss.accept()
          var host = s.getInetAddress.getHostName
          println("push connection established from : " + s.getInetAddress.getHostName)

          var r = new Receiver(Server.this, s)
          //print("starting receiver")
          r.start()
        }
        catch {
          case toe: SocketTimeoutException =>
          case se: SocketException =>
        }
      }
      println("push service stopped")
    }
  }

  class FetchService extends Thread {
    val ss: ServerSocket = new ServerSocket(0)
    fetchServicePort = ss.getLocalPort
    ss.setSoTimeout(1000000)

    def stopWork() {
      ss.close()
    }

    override def run() {

      println("fetching service started")
      while (running) {
        try {
          val s: Socket = ss.accept()
          var host = s.getInetAddress.getHostName
          println("fetch connection established from : " + s.getInetAddress.getHostName)

          var r = new Sender(Server.this, s)
          //println("starting sender")
          r.start()
        }
        catch {
          case toe: SocketTimeoutException =>
          case se: SocketException =>
        }
      }
      println("fetch service stopped")
    }
  }
}
