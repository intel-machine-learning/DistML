package com.intel.distml.util.scala

import com.intel.distml.api.{Session, DMatrix}
import com.intel.distml.util.{KeyCollection, DataDesc}

import scala.collection.mutable

/**
 * Created by yunlong on 1/3/16.
 */
abstract class SparseArray[K, V] (
dim : Long,
keyType : Int,
valueType : Int) extends DMatrix(dim) {

  format = new DataDesc(DataDesc.DATA_TYPE_ARRAY, keyType, valueType)

  def fetch(rows: KeyCollection, session: Session): mutable.HashMap[K, V] = {
    val result = new mutable.HashMap[K, V]
    val data: Array[Array[Byte]] = session.dataBus.fetch(name, rows, format)
    for (obj <- data) {
      val m: mutable.HashMap[K, V] = readMap(obj)
      m.foreach( f => result += f )
    }
    return result
  }

  def push(data: mutable.HashMap[K, V], session: Session) {
    val bufs = new Array[Array[Byte]](partitions.length)

    for (i <- 0 to partitions.length - 1) {
      val p: KeyCollection = partitions(i)
      val m: mutable.HashMap[K, V] = new mutable.HashMap[K, V]
      for (key <- data.keySet) {
        if (p.contains(toLong(key))) m.put(key, data(key))
      }
      bufs(i) = writeMap(m)
    }
    session.dataBus.push(name, format, bufs)
  }

  private def readMap(buf: Array[Byte]): mutable.HashMap[K, V] = {
    val data = new mutable.HashMap[K, V]
    var offset: Int = 0
    while (offset < buf.length) {
      val key: K = format.readKey(buf, offset).asInstanceOf[K]
      offset += format.keySize
      val value: V = format.readValue(buf, offset).asInstanceOf[V]
      offset += format.valueSize
      data.put(key, value)
    }
    return data
  }

  private def writeMap(data: mutable.HashMap[K, V]): Array[Byte] = {
    val recordLen: Int = format.keySize + format.valueSize
    val buf: Array[Byte] = new Array[Byte](recordLen * data.size)
    var offset: Int = 0
    for ((k, v) <- data) {
      format.writeKey(k.asInstanceOf[Number], buf, offset)
      offset += format.keySize
      offset = format.writeValue(v, buf, offset)
    }
    return buf
  }

  protected def toLong(k : K) : Long

}
