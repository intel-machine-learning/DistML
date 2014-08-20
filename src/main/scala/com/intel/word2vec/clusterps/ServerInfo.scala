package com.intel.word2vec.clusterps

/**
 * Created by spark on 7/3/14.
 */
class ServerInfo (
val fromIndex : Int,
val toIndex : Int,
var address : String,
var pushServicePort : Int,
var fetchServicePort : Int
) extends Serializable {
}
