package com.intel.distml.platform

/**
 * Created by yunlong on 2/3/16.
 */
class Clock(name : String) {

  var startTime = 0L
  var total = 0L

  def reset(): Unit = {
    total = 0L
    startTime = 0L
  }

  def start(): Unit = {
    total = 0L
    startTime = System.currentTimeMillis()
  }

  def stop(): Unit = {
    total += System.currentTimeMillis() - startTime
    println("[" + name + "] " + total + " ms")
    reset
  }

  def pause(): Unit = {
    total += System.currentTimeMillis() - startTime
  }

  def resume(): Unit = {
    startTime = System.currentTimeMillis()
  }
}
