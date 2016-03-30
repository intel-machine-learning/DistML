package com.intel.distml.clustering

import com.intel.distml.api.Model
import com.intel.distml.util.{IntMatrixWithIntKey, IntArrayWithIntKey}

/**
 * Created by yunlong on 16-3-29.
 */
class LDAModel(
val V : Int = 0,
val K: Int = 20,
val alpha : Double = 0.01,
val beta : Double = 0.01
) extends Model {

  val alpha_sum = alpha * K
  val beta_sum = beta * V

  registerMatrix("doc-topics", new IntArrayWithIntKey(K))
  registerMatrix("word-topics", new IntMatrixWithIntKey(V, K))


}
