package com.intel.word2vec.common

import java.io.DataInputStream
import org.apache.hadoop.conf.Configuration
import java.net.URI
import org.apache.hadoop.fs.{Path, FileSystem}

/**
 * Created by He Yunlong on 7/26/14.
 */
object IOHelper {

  def readInt(dis : DataInputStream) : Int = {
    while (dis.available() < 4) {
      Thread.sleep(1)
    }
    return dis.readInt()
  }

  def deleteHDFS(path : String) {
    var conf = new Configuration()
    val p = URI.create(path)
    var fs = FileSystem.get(p, conf)
    var dst = new Path(path)
    fs.delete(dst)
  }
}
