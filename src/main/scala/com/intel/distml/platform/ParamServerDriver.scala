package com.intel.distml.platform

import akka.actor.ActorSystem
import com.intel.distml.api.Model
import com.intel.distml.model.word2vec.{Word2VecModelWriter, Word2VecModel}
import com.typesafe.config.ConfigFactory
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * Created by yunlong on 6/3/15.
 */
class ParamServerDriver (@transient spark : SparkContext, actorSystemConfig : String, monitor : String, modelBroadcast : Broadcast[Model], psCount : Int) extends Thread with Serializable {

  def paramServerTask(modelBroadcast: Broadcast[Model])(index: Int) : Int = {

    println("starting server task")

    val PARAMETER_SERVER_ACTOR_SYSTEM_NAME = "parameter-server-system"
    val PARAMETER_SERVER_ACTOR_NAME = "parameter-server"

    // Start actor system
    val parameterServerRemoteConfig = ConfigFactory.parseString(actorSystemConfig)
    val parameterServerActorSystem = ActorSystem(PARAMETER_SERVER_ACTOR_SYSTEM_NAME + index,
      ConfigFactory.load(parameterServerRemoteConfig))

    // Start parameter server
    val parameterServer = parameterServerActorSystem.actorOf(ParameterServerActor.props(monitor, modelBroadcast, index),
      PARAMETER_SERVER_ACTOR_NAME)

    parameterServerActorSystem.awaitTermination()

    println("stopping server task")
    1
  }

  override def run() {
    var da = new Array[Int](psCount)
    for (i <- 0 to psCount - 1)
      da(i) = i

    val data = spark.parallelize(da, psCount)
    val dummy = data.map(paramServerTask(modelBroadcast)).collect()
    print("parameter servers finish their work.")
  }

}

