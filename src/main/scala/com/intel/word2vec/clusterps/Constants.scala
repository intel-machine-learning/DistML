package com.intel.word2vec.clusterps

/**
 * Created by He Yunlong on 7/3/14.
 */
object Constants {

  // network constants
  val MONITOR_PORT = 9990
  //val PARAM_SERVER_PUSH_PORT = 9989
  //val PARAM_SERVER_FETCH_PORT = 9988

  val PARAM_SERVER_COUNT = 6

  // driver and monitor constants
  val NODE_TYPE_DRIVER = 1
  val NODE_TYPE_PARAM_SERVER = 2
  val NODE_TYPE_WORKER = 3

  // push constants
  val PUSH_NEW_DATA_INDICATOR = 1   //sent by worket to indicate new data to push, followed by data count
  val PUSH_DONE_INDICATOR = 1    //sent by server to indicate all data received, worker can send new data after it
  val PUSH_CLOSE_INDICATOR = Integer.MAX_VALUE   //sent by worker to indicate no data to push, connection will be closed

  // fetch constants
  val FETCH_NEW_FETCH_INDICATOR = 1   //sent by worket to indicate new fetch request, followed by indexes to fetch
  val FETCH_DONE_INDICATOR = 1    //sent by worker to indicate all data received,
  val FETCH_CLOSE_INDICATOR = Integer.MAX_VALUE   //sent by worker to indicate no data to fetch, connection will be closed

  val DEBUG_PUSH = false
  val DEBUG_PUSH_PROGRESS = false
  val DEBUG_FETCH = false
  val DEBUG_FETCH_PROGRESS = false

  val MODEL_DIMENSION = 200
  val MODEL_PARAM_SIZE = MODEL_DIMENSION * 4
  val MIN_WORD_FREQ = 250

  val initialAlpha = 0.0025f
  val alphaThreshold = 0.0001f


}
