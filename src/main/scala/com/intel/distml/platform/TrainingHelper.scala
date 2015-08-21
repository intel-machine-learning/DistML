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
      |akka.remote.netty.tcp.maximum-frame-size=4126935
    """.stripMargin

  var monitorActorPath = ""
  var dataSetImmutable = true

//  def startTraining[T:ClassTag](spark : SparkContext, model : Model, samples: RDD[T], config : TrainingContext,modelWriter: ModelWriter = new DefaultModelWriter()): Unit = {
//
//    startTraining(spark, model, samples, config, modelWriter);
//    dataSetImmutable = model.dataSetImmutable
//
//  }

  def startTraining[T:ClassTag](spark : SparkContext, model : Model, samples: RDD[T], config : TrainingContext, modelWriter : ModelWriter=new DefaultModelWriter): Unit = {

    dataSetImmutable = model.dataSetImmutable

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
      new DistMLListener(spark, samples, modelBroadcast, config, modelWriter)), WORKER_STARTER_ACTOR_NAME)

    startParameterServers(spark, modelBroadcast, monitorActorPath, config.psCount);

    systemLog("waiting monitor to die")
    monitorActorSystem.awaitTermination();
    systemLog("monitor dies")
  }

  def startParameterServers(spark : SparkContext, modelBroadcast : Broadcast[Model], monitor : String, psCount : Int): Unit = {
    (new ParamServerDriver(spark, modelBroadcast, ACTOR_SYSTEM_CONFIG, monitor, psCount)).start()
  }

  class DistMLListener[T:ClassTag] (spark : SparkContext, samples: RDD[T], modelBroadcast : Broadcast[Model],
                                    context : TrainingContext, modelWriter : ModelWriter) extends WorkerStarter.Callback with Serializable{
/*
    override def monitorReady(): Unit = {
      // start parameter servers
      startParameterServers(spark, monitorActorPath, context.psCount);
    }
*/
    override def parameterServersReady(): Unit = {

      println("parameter servers are ready, start training now. ")
      //(new WorkerDriver(spark, ACTOR_SYSTEM_CONFIG, monitorActorPath, model, samples, config, modelWriter)).start()
      var tmp = samples;
      for (iter <- 0 to context.iteration - 1) {
        systemLog("iteration " + iter)
        context.currentIter = iter
        if (dataSetImmutable) {
          samples.mapPartitionsWithIndex(workerStartFunction(modelBroadcast, monitorActorPath, context)).collect
        }
        else {
          tmp = tmp.mapPartitionsWithIndex(workerStartFunction(modelBroadcast, monitorActorPath, context)).repartition(context.workerCount)
          tmp.collect

        }
        systemLog("iteration done " + iter)
      }

      systemLog("training work done")
    }
  }


  def startWorkers[T:ClassTag](spark : SparkContext, samples: RDD[T], modelBroadcast : Broadcast[Model],config : TrainingContext, modelWriter : ModelWriter) {
    println("start new iteration.");
    samples.mapPartitionsWithIndex(workerStartFunction(modelBroadcast, monitorActorPath, config)).collect
  }

  def workerStartFunction[T] (modelBroadcast : Broadcast[Model],monitorPath : String, config : TrainingContext)
                          (index : Int, samples: Iterator[T]) : Iterator[T] = {

    systemLog("starting worker " + index);
    val results = new MyDList[T]

    // Init actor system configurations
    val WORKER_ACTOR_SYSTEM_NAME = "worker-system"
    val WORKER_LEAD_ACTOR_NAME = "worker-lead"
    val WORKER_ACTOR_NAME = "worker"

    // Start actor system
    val workerRemoteConfig = ConfigFactory.parseString(ACTOR_SYSTEM_CONFIG)
    val workerActorSystem = ActorSystem(WORKER_ACTOR_SYSTEM_NAME + index, ConfigFactory.load(workerRemoteConfig))

    val worker = workerActorSystem.actorOf(WorkerActor.props(modelBroadcast.value, monitorPath, index,
      JavaConversions.asJavaIterator(samples), config, results), WORKER_ACTOR_NAME)

    systemLog("Worker Actor System Started")

    //Wait until actor system exit, the work must have been done then.
    workerActorSystem.awaitTermination()

    results.iterator()
  }

  private def systemLog(msg: String) {
    Logger.InfoLog("****************** " + msg + " ******************", Logger.Role.SYSTEM, 0)
  }
}
