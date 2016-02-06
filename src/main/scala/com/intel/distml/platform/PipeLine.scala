package com.intel.distml.platform

/**
 * Created by yunlong on 2/4/16.
 */
class PipeLine(tasks : Task, scheduler : Scheduler) extends Thread {

  override def run(): Unit = {
    var finished = false
    while(!finished) {
      var t = tasks

      println("run piple line now: " + t.resource)
      while((!finished) && (t != null)) {
        while(!scheduler.getResource(t)) {
          Thread.sleep(1000)
        }
        println("starting task: " + t.name)
        finished = t.run()
        println("task: " + t.name + "done with result: " + finished)
        scheduler.releaseResource(t)
        t = t.nextTask
      }
    }
  }
}

class Scheduler(var cpu : Int = 1, var network : Int = 1) {

  def getResource(t : Task) : Boolean = {
    //println("get resource: " + cpu + ", " + network + ", " + t.resource + ", " + Thread.currentThread().getId)
    if (t.resource == Task.CPU)
      getCPU()
    else {
      getNetwork()
    }
  }

  def releaseResource(t : Task) : Unit = {
    if (t.resource == Task.CPU)
      releaseCPU()
    else
      releaseNetwork()
  }

  private[Scheduler] def getCPU() : Boolean = {
     synchronized {
       if (cpu > 0) {
         cpu -= 1
         true
       }
       else false
     }
  }

  private[Scheduler] def releaseCPU() : Unit = {
    synchronized {
      cpu += 1
    }
  }

  private[Scheduler] def getNetwork() : Boolean = {
    synchronized {
      //println("get network: " + network)
      if (network > 0) {
        network -= 1
        true
      }
      else false
    }
  }

  private[Scheduler] def releaseNetwork() : Unit = {
    synchronized {
      network += 1
    }
  }
}

abstract class Task(val name : String, val resource : Int) extends Serializable {

  var nextTask : Task = null

  def run() : Boolean
}

object Task {
  final val NETWORK = 1
  final val CPU = 0
}