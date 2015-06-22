package com.intel.distml.model.demo_tao;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.io.Tcp;
import akka.io.TcpMessage;
import akka.util.ByteString;
import com.typesafe.config.ConfigFactory;

import java.net.InetSocketAddress;

/**
 * Created by taotaotheripper on 2015/6/10.
 */

public class Server extends UntypedActor {

    @Override
    public void preStart() throws Exception {
        final ActorRef tcp = Tcp.get(getContext().system()).manager();
        tcp.tell(TcpMessage.bind(getSelf(),
                new InetSocketAddress("localhost", 1234), 100), getSelf());
    }

    @Override
    public void onReceive(Object msg) throws Exception {
        if (msg instanceof Tcp.Bound) {
            System.out.println("Server bound");
        } else if (msg instanceof Tcp.CommandFailed) {
            System.out.println("Server bound failed");
            getContext().stop(getSelf());
        } else if (msg instanceof Tcp.Connected) {
            getSender().tell(TcpMessage.register(getSelf()), getSelf());
        } else if (msg instanceof Tcp.Received) {
            System.out.println("Message received by tcp");
        } else if (msg instanceof ByteString) {
            System.out.println("Message received by akka");
        } else if (msg instanceof Tcp.ConnectionClosed) {
            getContext().stop(getSelf());
        }
    }

    public static void run() {
        final String config = ""
                + "akka.loglevel = \"INFO\"\n"
                + "akka.stdout-loglevel = \"INFO\"\n"
                + "akka.actor.provider = \"akka.remote.RemoteActorRefProvider\"\n"
                + "akka.remote.netty.tcp.port = 4321\n"
                + "akka.remote.netty.tcp.maximum-frame-size = 1024000000b";
        ActorSystem serverSystem = ActorSystem.create("ServerSystem", ConfigFactory.parseString(config));
        ActorRef server = serverSystem.actorOf(Props.create(Server.class), "Server");
        serverSystem.awaitTermination();
    }
}