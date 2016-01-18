package com.intel.distml.platform

import akka.actor.ActorSystem
import com.intel.distml.api.Model
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
                             f : Function1[Model, T]) extends Thread with Serializable {
/*
  var finalResult : RDD[T] = null

  class PSStartThread(index : Int) extends Thread with Serializable {

    var result : RDD[T] = null

    override def run(): Unit = {
      var prefix = System.getenv("PS_NETWORK_PREFIX")

      var da = new Array[Int](1)
      da(0) = index
      val data = spark.parallelize(da, 1)
      result = data.map(paramServerTask(modelBroadcast, prefix, f))

      println("submitting parameter server task: " + index)
      result.count()
    }

  }
*/
  def paramServerTask(modelBroadcast : Broadcast[Model], prefix : String, f : Function1[Model, T])(index: Int) : T = {

    println("starting server task: " + index)

    val PARAMETER_SERVER_ACTOR_SYSTEM_NAME = "parameter-server-system"
    val PARAMETER_SERVER_ACTOR_NAME = "parameter-server"

    // Start actor system
    val parameterServerRemoteConfig = ConfigFactory.parseString(actorSystemConfig)
    val parameterServerActorSystem = ActorSystem(PARAMETER_SERVER_ACTOR_SYSTEM_NAME + index,
      ConfigFactory.load(parameterServerRemoteConfig))

    // Start parameter server
    val model = modelBroadcast.value
    val parameterServer = parameterServerActorSystem.actorOf(ParameterServerActor.props(model, monitor, index, prefix),
      PARAMETER_SERVER_ACTOR_NAME)

    parameterServerActorSystem.awaitTermination()

    println("stopping server task")

    f(model)
  }

  override def run() {
    var prefix = System.getenv("PS_NETWORK_PREFIX")

    var da = new Array[Int](psCount)
    for (i <- 0 to psCount - 1)
      da(i) = i

    val data = spark.parallelize(da, psCount)
    println("prepare to start parameter servers: " + data.partitions.length)
    //val dummy = data.map(paramServerTask(modelBroadcast, prefix, f)).collect()
    val dummy = data.map(paramServerTask(modelBroadcast, prefix, f)).collect()
    println("parameter servers finish their work.")
  }
/*
  override def run(): Unit = {

    val starters = new Array[PSStartThread](psCount)
    for (i <- 0 to psCount-1) {
      println("starting parameter servers: " + i)
      starters(i) = new PSStartThread(i)
      starters(i).start()
    }
    println("waiting parameter servers to stop: " + psCount)
    for (i <- 0 to psCount-1) {
      starters(i).join()
    }

    finalResult = starters(0).result
    for (i <- 1 to psCount-1) {
      finalResult = finalResult.union(starters(i).result)
    }
  }
*/
}

