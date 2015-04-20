package com.intel.distml.platform

import akka.actor._
import akka.event.Logging

/**
 * Created by yunlong on 4/17/15.
 */
object KryoSerializerExt extends ExtensionId[KryoSerialization] with ExtensionIdProvider {
  override def get(system: ActorSystem): KryoSerialization = super.get(system)

  override def lookup = KryoSerializerExt

  override def createExtension(system: ExtendedActorSystem): KryoSerialization = new KryoSerialization(system)
}

class KryoSerialization(val system: ExtendedActorSystem) extends Extension {

//  val settings = new Settings(system)
  val log = Logging(system, getClass.getName)
}