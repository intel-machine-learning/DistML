package com.intel.distml.platform

import java.util

import scala.reflect.runtime.universe._
import scala.reflect._

import com.intel.distml.api.{DMatrix, DefaultModelWriter, ModelWriter, Model}
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions
import org.apache.spark.SparkContext
import com.intel.distml.platform.server.ParameterServerActor
import com.intel.distml.platform.worker.WorkerLeadActor
import com.intel.distml.platform.worker.WorkerActor

import com.intel.distml.util.{Matrix, Logger, IOHelper}

import com.intel.distml.platform.monitor._

import akka.actor._
import com.typesafe.config._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object TrainingHelper  {

  // Shutdown of sub actor system should not be treated as error, so we should set
  // akka.remote.log-remote-lifecycle-events=off
  // Turn on akka.log-dead-letters when really needed, such as debugging
  var ACTOR_SYSTEM_CONFIG =
    """
      |akka.actor.provider="akka.remote.RemoteActorRefProvider"
      |akka.remote.netty.tcp.port=0
      |akka.remote.log-remote-lifecycle-events=off
      |akka.log-dead-letters=off
      |akka.remote.netty.tcp.maximum-frame-size=500000000
      |akka.remote.transport-failure-detector.acceptable-heartbeat-pause=240s
    """.stripMargin

  ACTOR_SYSTEM_CONFIG =
    """
      |akka.actor.provider="akka.remote.RemoteActorRefProvider"
      |akka.remote.netty.tcp.port=0
      |akka.remote.log-remote-lifecycle-events=off
      |akka.log-dead-letters=off
      |akka.remote.netty.tcp.maximum-frame-size=500000000
      |akka.remote.transport-failure-detector.acceptable-heartbeat-pause=240s
      |akka.extensions=["com.intel.distml.platform.KryoSerializerExt"]
      |akka.actor.serializers.kryo="com.intel.distml.platform.KryoSerializer"
      |akka.actor.kryo {
      | // implicit-registration-logging=true
      | // kryo-trace=true
      |}
      |akka.actor.serialization-bindings {
      | "com.intel.distml.transport.DataBusProtocol$ScamlMessage"=kryo
      |}
    """.stripMargin

  def startTraining[T:ClassTag](spark : SparkContext, model : Model, samples: RDD[T], config : TrainingConf): Unit = {

    startTraining(spark, model, samples, config, new DefaultModelWriter());

  }

  def startTraining[T:ClassTag](spark : SparkContext, model : Model, samples: RDD[T], config : TrainingConf, modelWriter : ModelWriter): Unit = {

    model.partitionParams(config.psCount);

    val params = new java.util.HashMap[String, Matrix]()
    for (iter <- 0 to config.iteration - 1) {
      params.clear()
      val it = model.dataMap.entrySet().iterator()
      while (it.hasNext) {
        val entry = it.next()
        val matrixName = entry.getKey
        val matrix = entry.getValue

        if ((matrix.`type` == DMatrix.TYPE_PARAM) && (matrix.localCache != null)) {
          params.put(matrixName, matrix.localCache)
        }
      }
      model.clearAllCache()

      systemLog("start new iteration: " + iter)

      startIteration(spark, model, params, samples, config, modelWriter)
    }

  }

  def startIteration[T:ClassTag](spark : SparkContext, model : Model, params : util.HashMap[String, Matrix], samples: RDD[T], config : TrainingConf, modelWriter : ModelWriter) {

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

    // Start actor system
    val monitorRemoteConfig = ConfigFactory.parseString(ACTOR_SYSTEM_CONFIG)
    val monitorActorSystem = ActorSystem(MONITOR_ACTOR_SYSTEM_NAME, ConfigFactory.load(monitorRemoteConfig))

    // Start monitor
    val monitorActorRef = monitorActorSystem.actorOf(MonitorActor.props(model, params, config, modelWriter), MONITOR_ACTOR_NAME)

    // Get monitor actor path
    val address = monitorActorSystem.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress
    val monitorActorPath = monitorActorRef.path.toSerializationFormatWithAddress(address)
    systemLog("Monitor Actor System Started")

    val tmp = samples.repartition(config.groupSize * config.groupCount)

    val data2 = samples.take(config.psCount)
    val rdd2 : RDD[T] = spark.parallelize(data2, config.psCount)
    val newSamples = tmp.union(rdd2)
    systemLog("new sample partitions: " + newSamples.partitions.length)

    newSamples.mapPartitionsWithIndex(startFunction(monitorActorPath, model, config)).collect

    systemLog("waiting monitor to die")
    monitorActorSystem.awaitTermination();
    systemLog("monitor dies")
  }

  def paramInfo[T: TypeTag](x: T): Unit = {
    val targs = typeOf[T] match { case TypeRef(_, _, args) => args }
    println(s"type of $x has type arguments $targs")
  }

  def makeArray[T : reflect.ClassTag](length: Int): Array[T] = {
    val tTag = implicitly[reflect.ClassTag[T]]
    tTag.newArray(length)
  }

  def startFunction[T] (monitorPath : String, model : Model, config : TrainingConf)
                             (index : Int, samples: Iterator[T]) : Iterator[Int] = {

    val wc = config.groupCount * config.groupSize

    if (index >= wc ) {
      parameterStartFunction(monitorPath, model, index - wc)
    }
    else {
      workerStartFunction(monitorPath, model, config)(index, samples)
    }
  }

  def parameterStartFunction (monitorPath : String, model : Model, index : Int) : Iterator[Int] = {
    // Init actor system configurations
    val PARAMETER_SERVER_ACTOR_SYSTEM_NAME = "parameter-server-system"
    val PARAMETER_SERVER_ACTOR_NAME = "parameter-server"

    // Start actor system
    val parameterServerRemoteConfig = ConfigFactory.parseString(ACTOR_SYSTEM_CONFIG)
    val parameterServerActorSystem = ActorSystem(PARAMETER_SERVER_ACTOR_SYSTEM_NAME + index,
      ConfigFactory.load(parameterServerRemoteConfig))

    // Start parameter server
    val parameterServer = parameterServerActorSystem.actorOf(ParameterServerActor.props(monitorPath, model, index),
      PARAMETER_SERVER_ACTOR_NAME)
    systemLog("Parameter Server Actor System Started")

    parameterServerActorSystem.awaitTermination()

    // return a dummy result
    val dummy = new Array[Int](0)
    dummy.iterator

  }

  def workerStartFunction[T] (monitorPath : String, model : Model, config : TrainingConf)
                          (index : Int, samples: Iterator[T]) : Iterator[Int] = {

    systemLog("starting worker " + index);

    // Init actor system configurations
    val WORKER_ACTOR_SYSTEM_NAME = "worker-system"
    val WORKER_LEAD_ACTOR_NAME = "worker-lead"
    val WORKER_ACTOR_NAME = "worker"

    // Start actor system
    val workerRemoteConfig = ConfigFactory.parseString(ACTOR_SYSTEM_CONFIG)
    val workerActorSystem = ActorSystem(WORKER_ACTOR_SYSTEM_NAME + index, ConfigFactory.load(workerRemoteConfig))

    if (index % config.groupSize == 0) // Start worker lead
      workerActorSystem.actorOf(WorkerLeadActor.props(monitorPath, model,
        index / config.groupSize, config.miniBatchSize, config), WORKER_LEAD_ACTOR_NAME)

    // Start worker
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
