package com.intel.word2vec.clusterps

import scala.collection.mutable.LinkedList
import java.net.{SocketException, SocketTimeoutException, Socket, ServerSocket}
import scala.collection.mutable
import java.io.{DataOutputStream, DataInputStream}
import scala.util.Random
import com.intel.word2vec.common.{IOHelper, FloatOps}

/**
 * Created by He Yunlong on 7/12/14.
 */
class Server(
driverAddress : String,
vocabSize : Int
//wordTree : WordTree
) {

  var fromIndex = 0
  var toIndex = 0

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
    return partition(index - fromIndex)
  }

  def sendInfoToMonitor() {
    println("init parameter server: " + driverAddress)

    socket = new Socket(driverAddress, Constants.MONITOR_PORT)
    dis = new DataInputStream(socket.getInputStream())
    dos = new DataOutputStream(socket.getOutputStream())

    dos.writeInt(Constants.NODE_TYPE_PARAM_SERVER)
    dos.writeInt(pushServicePort)
    dos.writeInt(fetchServicePort)

    fromIndex = IOHelper.readInt(dis)
    toIndex = IOHelper.readInt(dis)
    println("fromIndex: " + fromIndex + ", toIndex: " + toIndex)

    partition = new Array[WordNodeData](toIndex - fromIndex + 1)

    var d : WordNodeData = null
    println("init nodes from " +fromIndex + " to " + toIndex)
    for (i <- fromIndex to toIndex) {
      d = new WordNodeData(Constants.MODEL_DIMENSION)
      d.index = i
      d.init(Constants.initialAlpha)
      partition(i-fromIndex) = d
      //wordTree.getWord(i).data = d
    }
    println("init done ")

    dos.writeInt(1)
    println("param server ready: " + "fromIndex: " + fromIndex + ", toIndex: " + toIndex)
  }

  def start() {
    println("starting parameter server")

    var p = new PushService()
    var f = new FetchService()
    sendInfoToMonitor()

    p.start()
    f.start()


    var workDoneIndicator = dis.read()
    println("workers have done their work, stopping myself")
    println("fromIndex="+fromIndex + ", " + "toIndex=" + toIndex + ", vocalsize=" + vocabSize)

    if (fromIndex < vocabSize) {
      var end = Math.min(toIndex, vocabSize-1)
      var count = end - fromIndex + 1
      dos.writeInt(count)
      println("param count: " + count)

      var dataBytes = new Array[Byte](Constants.MODEL_DIMENSION * 4)
      for (i <- fromIndex to end) {
        dos.writeInt(i)

        //var d = wordTree.getWord(i).data
        var d = partition(i - fromIndex)

        for (i <- 0 to Constants.MODEL_DIMENSION-1) {
          FloatOps.getFloatBytes(d.syn0(i), dataBytes, i*4)
        }
        dos.write(dataBytes)
      }

    }
    else {
      dos.writeInt(0)
    }

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
