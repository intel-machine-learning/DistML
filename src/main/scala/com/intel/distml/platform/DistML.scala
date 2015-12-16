package com.intel.distml.platform

import akka.actor._
import com.intel.distml.api.Model
import com.intel.distml.platform.MonitorActor.TrainingDone
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkContext

/**
 * Created by yunlong on 12/8/15.
 */

class DistML(
model : Model,
psCount : Int,
system: ActorSystem,
val monitorPath : String,
monitorActor : ActorRef
)
{

  def save(): Unit = {
    throw new Exception("Will be implemented soon")
  }

  def load(): Unit = {
    throw new Exception("Will be implemented soon")
  }

  def zero(): Unit = {
    throw new Exception("Will be implemented soon")
  }

  def random(): Unit = {
    throw new Exception("Will be implemented soon")
  }

  def recycle(): Unit = {
    monitorActor.tell(new TrainingDone(), null)

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

  def distribute(sc : SparkContext, model : Model, psCount : Int): DistML = {

    model.autoPartition(psCount);

    val MONITOR_ACTOR_SYSTEM_NAME = "monitor-system"
    val MONITOR_ACTOR_NAME = "monitor"

    // Start actor system
    val monitorRemoteConfig = ConfigFactory.parseString(ACTOR_SYSTEM_CONFIG)
    val monitorActorSystem = ActorSystem(MONITOR_ACTOR_SYSTEM_NAME, ConfigFactory.load(monitorRemoteConfig))

    val monitorActorRef = monitorActorSystem.actorOf(MonitorActor.props(model), MONITOR_ACTOR_NAME)

    val address = monitorActorSystem.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress
    model.monitorPath = monitorActorRef.path.toSerializationFormatWithAddress(address)

    var modelBroadcast = sc.broadcast(model)
    (new ParamServerDriver(sc, modelBroadcast, ACTOR_SYSTEM_CONFIG, model.monitorPath, psCount)).start()

    while (!model.psReady) {
      Thread.sleep(10)
    }
    println("=========== model distributed ==============");

    new DistML(model, psCount, monitorActorSystem, model.monitorPath, monitorActorRef)
  }

}
