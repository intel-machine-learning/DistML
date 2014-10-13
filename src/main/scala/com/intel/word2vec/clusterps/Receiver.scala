package com.intel.word2vec.clusterps

import java.net.Socket
import java.io.{DataOutputStream, DataInputStream}
import java.nio.ByteBuffer
import com.intel.word2vec.common.{IOHelper, FloatOps}


/**
 * Created by He Yunlong on 7/12/14.
 */
class Receiver(
server : Server,
//wordTree : WordTree,
socket : Socket
) extends Thread {

  var dis : DataInputStream = new DataInputStream(socket.getInputStream())
  var dos : DataOutputStream = new DataOutputStream(socket.getOutputStream())

  override def run() {
    //println("send buffer" + socket.getSendBufferSize())
    //println("receive buffer" + socket.getReceiveBufferSize())
    //socket.setSendBufferSize(Constants.MODEL_DIMENSION * 64)
    //socket.setReceiveBufferSize(Constants.MODEL_DIMENSION * 64)
    //println("send buffer" + socket.getSendBufferSize())
    //println("receive buffer" + socket.getReceiveBufferSize())

    var running = true
    while (running) {
      var ind = IOHelper.readInt(dis)
      if (ind == Constants.PUSH_CLOSE_INDICATOR) {
        running = false
      }
      else if (ind == Constants.PUSH_NEW_DATA_INDICATOR) {
        readData()
      }
    }

    if (Constants.DEBUG_PUSH_PROGRESS) {
      println("stop receiving delta from " + socket.getInetAddress.getHostName)
    }

    try {
      dis.close()
      dos.close()
      socket.close()
    }
    catch { case xe : Exception => }
  }


  def readData() {
    val startTime = System.currentTimeMillis()

    var data : WordNodeData = null

    var dataBytes = new Array[Byte](Constants.MODEL_DIMENSION * 4)
    var deltas = new TempNodeData()

    var count = IOHelper.readInt(dis)
    //println("delta to be received: " + count)

    for (i <- 0 to count-1) {
      var index = IOHelper.readInt(dis)

      data = server.getData(index)

      dis.readFully(dataBytes)
      deltas.deserialize(dataBytes)
      data.mergeDelta0((deltas))

      dis.readFully(dataBytes)
      deltas.deserialize(dataBytes)
      data.mergeDelta1((deltas))

      if (Constants.DEBUG_PUSH && (index == 1000)) {
        println("delta merged: index=" + index + ", syn=" + data.syn0(0) + ", delta=" + data.delta0(0)
          + ", alpha0=" + data.alpha0(0) + ", alpha1=" + data.alpha1(0))
      }
    }

    dos.writeInt(Constants.PUSH_DONE_INDICATOR)

    println("read delta done with time " + (System.currentTimeMillis() - startTime)/1000 +
          " count=" + count + " from " + socket.getInetAddress.getHostName+ "\n")
  }

}
