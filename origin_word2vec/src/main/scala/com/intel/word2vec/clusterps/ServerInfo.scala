package com.intel.word2vec.clusterps

/**
 * Created by spark on 7/3/14.
 */
class ServerInfo (
) extends Serializable {

  var serverIndex : Int = 0
  var address : String = ""
  var pushServicePort : Int = 0
  var fetchServicePort : Int = 0

  def accept(paramIndex : Int) : Boolean = {
    return (paramIndex % Constants.PARAM_SERVER_COUNT == serverIndex)
  }
}
