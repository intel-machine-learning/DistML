
package com.intel.word2vec.clusterps

import java.net.Socket
import java.io.{DataOutputStream, DataInputStream}
import java.nio.ByteBuffer
import com.intel.word2vec.common.{IOHelper, FloatOps}


/**
 * Created by He Yunlong on 7/12/14.
 */
class Sender (
server : Server,
//wordTree : WordTree,
socket : Socket
) extends Thread {

  var dis : DataInputStream = new DataInputStream(socket.getInputStream())
  var dos : DataOutputStream = new DataOutputStream(socket.getOutputStream())

  var dataBytes = new Array[Byte](Constants.MODEL_DIMENSION * 4)

  var partitionIndex = 0

  override def run() {

    var running = true
    while (running) {
      var ind = IOHelper.readInt(dis)
      if (ind == Constants.FETCH_CLOSE_INDICATOR) {
        running = false
      }
      else if (ind == Constants.FETCH_NEW_FETCH_INDICATOR) {
        partitionIndex = IOHelper.readInt(dis)
        var count = IOHelper.readInt(dis)
        if (Constants.DEBUG_FETCH)
          println("requested count: " + count + ", from " + socket.getInetAddress.getHostName)

        for(i <- 0 to count-1) {
          var index = IOHelper.readInt(dis)
          //println("requesting: " + index)
          var w = server.getData(index)
          sendBack(w)
        }

        if (Constants.DEBUG_FETCH)
          println("sending done: " + count)
      }
    }

    //println("closing connection with " + socket.getInetAddress.getHostName + ", " + partitionIndex)
    try {
      dis.close()
      dos.close()
      socket.close()
    }
    catch { case xe : Exception => }
  }

  def sendBack(data : WordNodeData) {

    dos.writeInt(data.index)

    for (i <- 0 to Constants.MODEL_DIMENSION-1) {
      FloatOps.getFloatBytes(data.syn0(i), dataBytes, i*4)
    }
    dos.write(dataBytes)
    for (i <- 0 to Constants.MODEL_DIMENSION-1) {
      FloatOps.getFloatBytes(data.syn1(i), dataBytes, i*4)
    }
    dos.write(dataBytes)

    for (i <- 0 to Constants.MODEL_DIMENSION-1) {
      FloatOps.getFloatBytes(data.alpha0(i), dataBytes, i*4)
    }
    dos.write(dataBytes)
    for (i <- 0 to Constants.MODEL_DIMENSION-1) {
      FloatOps.getFloatBytes(data.alpha1(i), dataBytes, i*4)
    }
    dos.write(dataBytes)
  }

}
