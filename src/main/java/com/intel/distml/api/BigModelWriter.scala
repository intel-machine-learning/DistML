package com.intel.distml.api

import com.intel.distml.api.databus.ServerDataBus
import com.intel.distml.util.{KeyRange, Matrix}

/**
 * Created by yunlong on 4/18/15.
 */
class BigModelWriter extends ModelWriter {

  val MAX_FETCH_SIZE = 1000000;

  def writeModel (model: Model, dataBus: ServerDataBus) {

    import scala.collection.JavaConversions._

    for (matrixName <- model.dataMap.keySet) {
      val m: DMatrix = model.dataMap.get (matrixName)
      if (m.`type` == DMatrix.TYPE_PARAM) {
        val size = m.getRowKeys.size()
        println("total param: " + size);

        m.setLocalCache(null)
        var start = 0L;
        while(start < size-1) {
          val end = Math.min(start + MAX_FETCH_SIZE, size) - 1
          val range = new KeyRange(start, end)
          println("fetch param: " + range);
          val result: Matrix = dataBus.fetchFromServer (matrixName, range)
          if (m.localCache == null) {
            m.setLocalCache (result)
          }
          else {
            println("merge to local cache.");
            m.localCache.mergeMatrix(result)
          }
          println("fetch done.");
          start = end + 1;
        }
        println("fetched param: " + matrixName + ", size=" + m.localCache.getRowKeys.size());
      }
    }
  }
}
