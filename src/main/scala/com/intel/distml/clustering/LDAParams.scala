package com.intel.distml.clustering

/**
 * Created by yunlong on 12/23/15.
 */
case class LDAParams(
var psCount : Int = 2,
var batchSize : Int = 100,
var input: String = null,
var k: Int = 20,
var alpha : Double = 0.01,
var beta : Double = 0.01,
var maxIterations: Int = 10,
val partitions : Int = 2,
var showPlexity: Boolean = true)
{

  var V : Int = 0
  var alpha_sum : Double = 0.0
  var beta_sum : Double = 0.0

  def init(V : Int): Unit = {
    this.V = V
    alpha_sum = alpha * k
    beta_sum = beta * V
  }
}
