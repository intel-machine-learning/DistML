package com.intel.distml.util.scala

import com.intel.distml.api.{DMatrix, Session}
import com.intel.distml.util.{DataDesc, KeyCollection, KeyRange}

import scala.collection.mutable

/**
 * Created by yunlong on 1/3/16.
 */
abstract class SparseMatrixAdapGrad[K, V] (
dim : Long,
cols: Int,
keyType : Int,
valueType : Int) extends DMatrix(dim) {

  format = new DataDesc(DataDesc.DATA_TYPE_MATRIX, keyType, valueType, false, true, true)
  val colKeys = new KeyRange(0, cols - 1)

  override def getColKeys() = colKeys

  def fetch(rows: KeyCollection, session: Session): mutable.HashMap[K, (Array[V], Array[Float])] = {
    val result = new mutable.HashMap[K, (Array[V], Array[Float])]
    val data = session.dataBus.fetch(name, rows, format)
    for (obj <- data) {
      val m: mutable.HashMap[K, (Array[V], Array[Float])] = readMap(obj)
      m.foreach( f => result += f )
    }
    return result
  }

  def push(data: mutable.HashMap[K, Array[V]], session: Session) {
    val bufs = new Array[Array[Byte]](partitions.length)

    for (i <- 0 to partitions.length - 1) {
      val p = partitions(i)
      val m = new mutable.HashMap[K, Array[V]]
      for (key <- data.keySet) {
        if (p.contains(toLong(key))) m.put(key, data(key))
      }
      bufs(i) = writeMap(m)
    }
    session.dataBus.push(name, format, bufs)
  }

  def cloneData(data: mutable.HashMap[K, (Array[V], Array[Float])]): mutable.HashMap[K, Array[V]] = {
    val t = new mutable.HashMap[K, Array[V]]
    for ((k, v) <- data) {

      val tv = createValueArray(v._1.length)
      for (i <- 0 to v._1.length-1)
        tv(i) = v._1(i)
//      System.arraycopy(tv, 0, v._1, 0, v._1.length)
      t.put(k, tv)
    }
    t
  }

  def getUpdate(newData: mutable.HashMap[K, (Array[V], Array[Float])], oldData: mutable.HashMap[K, Array[V]]) {
    //println("get update: " + oldData.size)
    for ((k, v) <- newData) {
      val tv = oldData.get(k).get
      for (i <- 0 to v._1.length-1) {
        tv(i) = subtract(v._1(i), tv(i))
      }
    }

    var keys = oldData.keySet
    for (k <- keys) {
      var v = oldData.get(k).get

      var i = 0
      while((i < v.length) && isZero(v(i))) {
        i += 1
      }
      if (i == v.length) {
        oldData.remove(k)
      }
//      else {
//        println("update(" + k + ")(0) = " + v(0))
//      }
    }
    //println("get update done: " + oldData.size)
  }

  private def readMap(buf: Array[Byte]): mutable.HashMap[K, (Array[V], Array[Float])] = {
    //println("read map: " + buf.length)
    val data = new mutable.HashMap[K, (Array[V], Array[Float])]
    var offset: Int = 0
    while (offset < buf.length) {
      //println("read offset: " + offset)
      val key: K = format.readKey(buf, offset).asInstanceOf[K]
      offset += format.keySize
      val values = createValueArray(colKeys.size.toInt)
      val alphas = new Array[Float](colKeys.size.toInt)
      if (format.denseColumn) {
        for (i <- 0 to colKeys.size.toInt - 1) {
          //println("read offset: " + offset + ", " + i)
          values(i) = format.readValue(buf, offset).asInstanceOf[V]
          offset += format.valueSize
          alphas(i) = format.readFloat(buf, offset)
          offset += 4
        }
      }
      else {
        val count: Int = format.readInt(buf, offset)
        offset += 4
        for (i <- 0 to count -1 ) {
          val index: Int = format.readInt(buf, offset)
          offset += 4
          values(i) = format.readValue(buf, offset).asInstanceOf[V]
          offset += format.valueSize
          alphas(i) = format.readFloat(buf, offset)
          offset += 4
        }
      }

      data.put(key, (values, alphas))
    }
    return data
  }

  private def writeMap(data: mutable.HashMap[K, Array[V]]): Array[Byte] = {

    var buf: Array[Byte] = null

    if (format.denseColumn) {
      val len: Int = (format.valueSize * data.size * colKeys.size).toInt
      buf = new Array[Byte](format.keySize * data.size + len)
    }
    else {
      var nzcount: Int = 0
      for (values <- data.values) {
        for (value <- values) {
          if (!isZero(value)) {
            nzcount += 1
          }
        }
      }
      val len: Int = ((format.valueSize + 4) * nzcount).toInt
      buf = new Array[Byte](format.keySize * data.size + len)
    }

    var offset: Int = 0
    for ((k, v) <- data) {
      format.writeKey(k.asInstanceOf[Number], buf, offset)
      offset += format.keySize
      val values: Array[V] = v
      if (format.denseColumn) {
        for ( i <- 0 to colKeys.size.toInt - 1) {
          format.writeValue(values(i), buf, offset)
          offset += format.valueSize
        }
      }
      else {
        val counterIndex: Int = offset
        offset += 4
        var counter: Int = 0
        for (i <- 0 to values.length - 1) {
          val value: V = values(i)
          if (!isZero(value)) {
            format.write(i, buf, offset)
            offset += 4
            format.writeValue(value, buf, offset)
            offset += format.valueSize
          }
          counter += 1
        }
        format.write(counter, buf, counterIndex)
      }
    }
    return buf
  }

  protected def toLong(k : K) : Long

  protected def isZero(value: V): Boolean

  protected def subtract(value: V, delta : V): V

  protected def createValueArrayWithAlpha(size: Int): Array[(V, Float)]

  protected def createValueArray(size: Int): Array[(V)]
}
