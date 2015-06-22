package com.intel.distml.model.demo_tao;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.ConfigFactory;

/**
 * Created by lq on 6/22/15.
 */
public class Demo {
 public void run(){
     final String config1 = ""
             + "akka.loglevel = \"INFO\"\n"
             + "akka.stdout-loglevel = \"INFO\"\n"
             + "akka.actor.provider = \"akka.remote.RemoteActorRefProvider\"\n"
             + "akka.remote.netty.tcp.port = 4321\n"
             + "akka.remote.netty.tcp.maximum-frame-size = 1024000000b";
     final String config2 = ""
             + "akka.loglevel = \"INFO\"\n"
             + "akka.stdout-loglevel = \"INFO\"\n"
             + "akka.actor.provider = \"akka.remote.RemoteActorRefProvider\"\n"
             + "akka.remote.netty.tcp.port = 0\n"
             + "akka.remote.netty.tcp.maximum-frame-size = 1024000000b";
     ActorSystem serverSystem = ActorSystem.create("ServerSystem", ConfigFactory.parseString(config1));
     ActorRef server = serverSystem.actorOf(Props.create(Server.class), "Server");
     ActorSystem clientSystem = ActorSystem.create("ClientSystem", ConfigFactory.parseString(config2));
     ActorRef client = clientSystem.actorOf(Props.create(Client.class), "Client");
     clientSystem.awaitTermination();
 }
}
