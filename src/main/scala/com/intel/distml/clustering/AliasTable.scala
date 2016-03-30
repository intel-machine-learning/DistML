package com.intel.distml.clustering

import java.util
import java.util.Random

/**
 * Created by yunlong on 12/19/15.
 */
class AliasTable (K: Int) extends Serializable {

  var L: Array[Int] = new Array[Int](K)
  var H: Array[Int] = new Array[Int](K)
  var P: Array[Float] = new Array[Float](K)

  def sampleAlias(gen: Random): Int = {
    val s = gen.nextFloat() * K
    var t = s.toInt
    if (t >= K) t = K - 1
    if (K * P(t) < (s-t)) L(t) else H(t)
  }

  def init(probs : Array[(Int, Float)], mass : Float): Unit = {

    val m = 1.0 / K
    val lq = new util.LinkedList[(Int, Float)]()
    val hq = new util.LinkedList[(Int, Float)]()

    for ((k, _p) <- probs) {
      val p = _p / mass
      if (p < m) lq.add((k, p))
      else hq.add((k, p))

    }

    var index = 0
    while (!lq.isEmpty & !hq.isEmpty) {
      val (l, pl) = lq.removeFirst()
      val (h, ph) = hq.removeFirst()
      L(index) = l
      H(index) = h
      P(index) = pl
      val pd = ph - (m - pl)
      if (pd >= m) hq.add((h, pd.toFloat))
      else lq.add((h, pd.toFloat))
      index += 1
    }

    while (!hq.isEmpty) {
      val (h, ph) = hq.removeFirst()
      L(index) = h
      H(index) = h
      P(index) = ph
      index += 1
    }

    while (!lq.isEmpty) {
      val (l, pl) = lq.removeLast()
      L(index) = l
      H(index) = l
      P(index) = pl
      index += 1
    }
  }
}