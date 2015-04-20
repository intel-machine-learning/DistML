package com.intel.distml.util

import java.io.{ObjectOutputStream, FileOutputStream, File, DataInputStream}
import org.apache.hadoop.conf.Configuration
import java.net.URI
import org.apache.hadoop.fs.{Path, FileSystem}

object IOHelper {
  def readString(dis : DataInputStream) : String = {
    return dis.readUTF()
  }

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

  def writeToTemp(obj : AnyRef): Unit = {
    var f = new File("/tmp/scaml/")
    if (!f.exists()) {
      f.mkdirs()
    }

    var i = 0;
    while (i < 1000) {
      var f1 = new File(f, "" + i);
      if (!f1.exists()) {
        println("write obj: " + obj);
        val os = new ObjectOutputStream(new FileOutputStream(f1))
        os.writeObject(obj)
        os.close()
        i = 1000
      }

      i = i + 1
    }


  }
}
