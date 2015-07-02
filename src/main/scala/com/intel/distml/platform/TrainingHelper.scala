package com.intel.distml.platform

import com.intel.distml.api.{DMatrix, DefaultModelWriter, ModelWriter, Model}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions
import org.apache.spark.{SparkConf, SparkContext}

import com.intel.distml.util.{Matrix, Logger, IOHelper}


import akka.actor._
import com.typesafe.config._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object TrainingHelper {

  // Shutdown of sub actor system should not be treated as error, so we should set
  // akka.remote.log-remote-lifecycle-events=off
  // Turn on akka.log-dead-letters when really needed, such as debugging
  var ACTOR_SYSTEM_CONFIG =
    """
      |akka.actor.provider="akka.remote.RemoteActorRefProvider"
      |akka.remote.netty.tcp.port=0
      |akka.remote.log-remote-lifecycle-events=off
      |akka.log-dead-letters=off
      |akka.io.tcp.direct-buffer-size = 2 MB
      |akka.io.tcp.trace-logging=off
    """.stripMargin

  var monitorActorPath = ""

  def startTraining[T:ClassTag](spark : SparkContext, model : Model, samples: RDD[T], config : TrainingConf): Unit = {

    startTraining(spark, model, samples, config, new DefaultModelWriter());

  }

  def startTraining[T:ClassTag](spark : SparkContext, model : Model, samples: RDD[T], config : TrainingConf, modelWriter : ModelWriter): Unit = {

    config.workerCount(samples.partitions.length)

    model.autoPartition(config.psCount);
    println("broadcast model now");
    var modelBroadcast = spark.broadcast(model)

    println(" start training: " + config.totalSampleCount);
    if (config.totalSampleCount <= 0) {
      config.totalSampleCount = samples.count()
      systemLog("totalSampleCount = " + config.totalSampleCount)
    }

    if (config.progressStepSize <= 0) {
      config.progressStepSize = (config.totalSampleCount / 100).toInt;
      if (config.progressStepSize <= 0) {
        config.progressStepSize = 1;
      }
    }

    // Actor system configuration
    val MONITOR_ACTOR_SYSTEM_NAME = "monitor-system"
    val MONITOR_ACTOR_NAME = "monitor"
    val WORKER_STARTER_ACTOR_NAME = "workerStarter"

    // Start actor system
    val monitorRemoteConfig = ConfigFactory.parseString(ACTOR_SYSTEM_CONFIG)
    val monitorActorSystem = ActorSystem(MONITOR_ACTOR_SYSTEM_NAME, ConfigFactory.load(monitorRemoteConfig))

    // Start monitor
    val monitorActorRef = monitorActorSystem.actorOf(MonitorActor.props(spark.applicationId, model, config, modelWriter), MONITOR_ACTOR_NAME)

    // Get monitor actor path
    val address = monitorActorSystem.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress
    monitorActorPath = monitorActorRef.path.toSerializationFormatWithAddress(address)
    systemLog("Monitor Actor System Started")

    // Start monitor
    val workerStarterActorRef = monitorActorSystem.actorOf(WorkerStarter.props(monitorActorPath,
      new DistMLListener(spark, modelBroadcast, samples, config, modelWriter)), WORKER_STARTER_ACTOR_NAME)

    // start parameter servers
    startParameterServers(spark, monitorActorPath, modelBroadcast, config.psCount)

    systemLog("waiting monitor to die")
    monitorActorSystem.awaitTermination();
    systemLog("monitor dies")
  }

  def startParameterServers(spark : SparkContext, monitor : String, modelBroadcast : Broadcast[Model], psCount : Int): Unit = {
    (new ParamServerDriver(spark, ACTOR_SYSTEM_CONFIG, monitor, modelBroadcast, psCount)).start()
  }

  class DistMLListener[T:ClassTag] (spark : SparkContext, modelBroadcast : Broadcast[Model], samples: RDD[T],
                                    config : TrainingConf, modelWriter : ModelWriter) extends WorkerStarter.Callback with Serializable{

    override def parameterServersReady(): Unit = {

      println("parameter servers are ready, start training now.")
      //(new WorkerDriver(spark, ACTOR_SYSTEM_CONFIG, monitorActorPath, model, samples, config, modelWriter)).start()
      for (iter <- 0 to config.iteration - 1) {
        systemLog("iteration " + iter)
        samples.mapPartitionsWithIndex(workerStartFunction(monitorActorPath, modelBroadcast, config)).collect
      }
    }
  }


  def startWorkers[T:ClassTag](spark : SparkContext, modelBroadcast : Broadcast[Model], samples: RDD[T], config : TrainingConf, modelWriter : ModelWriter) {
    println("start new iteration.");
    samples.mapPartitionsWithIndex(workerStartFunction(monitorActorPath, modelBroadcast, config)).collect
  }

  def workerStartFunction[T] (monitorPath : String, modelBroadcast : Broadcast[Model], config : TrainingConf)
                          (index : Int, samples: Iterator[T]) : Iterator[Int] = {

    systemLog("starting worker " + index);

    // Init actor system configurations
    val WORKER_ACTOR_SYSTEM_NAME = "worker-system"
    val WORKER_LEAD_ACTOR_NAME = "worker-lead"
    val WORKER_ACTOR_NAME = "worker"

    // Start actor system
    val workerRemoteConfig = ConfigFactory.parseString(ACTOR_SYSTEM_CONFIG)
    val workerActorSystem = ActorSystem(WORKER_ACTOR_SYSTEM_NAME + index, ConfigFactory.load(workerRemoteConfig))

    // Start worker
    val model = modelBroadcast.value
    val worker = workerActorSystem.actorOf(WorkerActor.props(monitorPath, model, index,
      JavaConversions.asJavaIterator(samples), config), WORKER_ACTOR_NAME)

    systemLog("Worker Actor System Started")

    //Wait until actor system exit, the work must have been done then.
    workerActorSystem.awaitTermination()

    // return a dummy result
    val dummy = new Array[Int](0)
    dummy.iterator
  }

  private def systemLog(msg: String) {
    Logger.InfoLog("****************** " + msg + " ******************", Logger.Role.SYSTEM, 0)
  }
}
