package com.intel.distml.platform

import akka.actor.ActorSystem
import com.intel.distml.api.Model
import com.intel.distml.util.DataStore
import com.typesafe.config.ConfigFactory
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.reflect.ClassTag


/**
 * Created by yunlong on 6/3/15.
 */
class ParamServerDriver[T : ClassTag] (@transient spark : SparkContext,
                            modelBroadcast : Broadcast[Model],
                            actorSystemConfig : String,
                            monitor : String, psCount : Int,
                             f : Function3[Model, Int, java.util.HashMap[String, DataStore], T]) extends Thread with Serializable {
  var finalResult : RDD[T] = null

  def paramServerTask(modelBroadcast : Broadcast[Model], prefix : String, f : Function3[Model, Int, java.util.HashMap[String, DataStore], T])(index: Int) : T = {

    println("starting server task: " + index)

    val PARAMETER_SERVER_ACTOR_SYSTEM_NAME = "parameter-server-system"
    val PARAMETER_SERVER_ACTOR_NAME = "parameter-server"

    // Start actor system
    val parameterServerRemoteConfig = ConfigFactory.parseString(actorSystemConfig)
    val parameterServerActorSystem = ActorSystem(PARAMETER_SERVER_ACTOR_SYSTEM_NAME + index,
      ConfigFactory.load(parameterServerRemoteConfig))

    // Start parameter server
    val model = modelBroadcast.value
    val stores = DataStore.createStores(model, index)

    val parameterServer = parameterServerActorSystem.actorOf(PSActor.props(model, stores, monitor, index, prefix),
      PARAMETER_SERVER_ACTOR_NAME)

    parameterServerActorSystem.awaitTermination()

    println("stopping server task")

    f(model, index, stores)
  }

  override def run() {
    var prefix = System.getenv("PS_NETWORK_PREFIX")

    var da = new Array[Int](psCount)
    for (i <- 0 to psCount - 1)
      da(i) = i

    val data = spark.parallelize(da, psCount)
    println("prepare to start parameter servers: " + data.partitions.length)
    finalResult = data.map(paramServerTask(modelBroadcast, prefix, f)).cache
    finalResult.count()
    println("parameter servers finish their work.")
  }

}

