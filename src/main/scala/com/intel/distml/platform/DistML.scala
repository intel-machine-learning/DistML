package com.intel.distml.platform

import java.net.URI

import akka.actor._
import com.intel.distml.api.Model
import com.intel.distml.platform.MonitorActor.{SaveModel, LoadModel, TrainingDone}
import com.intel.distml.util.DataStore
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkContext

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.Path._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.collection.JavaConversions._

/**
 * Created by yunlong on 12/8/15.
 */

class DistML[T: ClassTag] (
model : Model,
psCount : Int,
system: ActorSystem,
val monitorPath : String,
monitorActor : ActorRef,
psDriverThread : ParamServerDriver[T]
)
{

  def params() : RDD[T] = {
    psDriverThread.finalResult
  }

  def save(path : String): Unit = {
    val req = new SaveModel(path)

    monitorActor.tell(req, null)
    while (!req.done) {
      Thread.sleep(10)
    }
  }

  def load(path : String): Unit = {

    val req = new LoadModel(path)

    monitorActor.tell(req, null)
    while (!req.done) {
      Thread.sleep(10)
    }
  }

  def zero(): Unit = {
    throw new Exception("Will be implemented soon")
  }

  def random(): Unit = {
    throw new Exception("Will be implemented soon")
  }

  def recycle(): Unit = {
    monitorActor.tell(new TrainingDone(), null)

    psDriverThread.join()
    system.awaitTermination()
  }

}

object DistML {

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

  def dummyF(model : Model, index : Int, stores : java.util.HashMap[String, DataStore]) : Int = {
    1
  }

  def defaultF(model : Model, index : Int, stores : java.util.HashMap[String, DataStore])
              : Iterator[(Int, String, DataStore)] = {

    println("collecting model parameters...")

    val map = new Array[(Int, String, DataStore)](stores.size())
    var i = 0
    for (name <- stores.keySet()) {
      val t : String = name
      map(i) = (index, t, stores.get(name))
      i += 1
    }

    println("done")
    map.iterator
  }

  def distribute(sc : SparkContext, model : Model, psCount : Int): DistML[Int] = {
    distribute[Int](sc, model, psCount, dummyF)
  }

  def distribute[T: ClassTag](sc : SparkContext, model : Model, psCount : Int,
                              f : Function3[Model, Int, java.util.HashMap[String, DataStore], T]): DistML[T] = {

    model.autoPartition(psCount)

    val MONITOR_ACTOR_SYSTEM_NAME = "monitor-system"
    val MONITOR_ACTOR_NAME = "monitor"

    // Start actor system
    val monitorRemoteConfig = ConfigFactory.parseString(ACTOR_SYSTEM_CONFIG)
    val monitorActorSystem = ActorSystem(MONITOR_ACTOR_SYSTEM_NAME, ConfigFactory.load(monitorRemoteConfig))

    val monitorActorRef = monitorActorSystem.actorOf(MonitorActor.props(model), MONITOR_ACTOR_NAME)

    val address = monitorActorSystem.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress
    model.monitorPath = monitorActorRef.path.toSerializationFormatWithAddress(address)

    var modelBroadcast = sc.broadcast(model)
    val psThread = new ParamServerDriver[T](sc, modelBroadcast, ACTOR_SYSTEM_CONFIG, model.monitorPath, psCount, f)
    psThread.start()

    while (!model.psReady) {
      Thread.sleep(10)
    }
    println("=========== model distributed ==============");

    new DistML[T](model, psCount, monitorActorSystem, model.monitorPath, monitorActorRef, psThread)
  }

}
